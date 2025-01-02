#include <rclcpp/rclcpp.hpp>
#include <gazebo_ros/node.hpp>
#include <rclcpp/logging.hpp>

#include <gazebo/physics/Model.hh>
#include <gazebo/physics/MultiRayShape.hh>// Store the latest laser scans into laserMsg
#include <gazebo/physics/physics.hh>
#include <gazebo/physics/PhysicsEngine.hh>
#include <gazebo/physics/World.hh>
#include <gazebo/sensors/GpuRaySensor.hh>
#include <gazebo/sensors/RaySensor.hh>
#include <gazebo/transport/Node.hh>
#include <chrono>
#include "ros2_livox/livox_points_plugin.h"
#include "ros2_livox/csv_reader.hpp"
#include <livox_interfaces/msg/custom_msg.hpp>

namespace gazebo
{

    GZ_REGISTER_SENSOR_PLUGIN(LivoxPointsPlugin)

    LivoxPointsPlugin::LivoxPointsPlugin() {}

    LivoxPointsPlugin::~LivoxPointsPlugin() {}

    void convertDataToRotateInfo(const std::vector<std::vector<double>> &datas, std::vector<AviaRotateInfo> &avia_infos)
    {
        avia_infos.reserve(datas.size());
        double deg_2_rad = M_PI / 180.0;
        for (auto &data : datas)
        {
            if (data.size() == 3)
            {
                avia_infos.emplace_back();
                avia_infos.back().time = data[0];
                avia_infos.back().azimuth = data[1] * deg_2_rad;
                avia_infos.back().zenith = data[2] * deg_2_rad - M_PI_2; //转化成标准的右手系角度
            } else {
            RCLCPP_ERROR(rclcpp::get_logger("convertDataToRotateInfo"), "data size is not 3!");
        }
        }
    }

    void LivoxPointsPlugin::Load(gazebo::sensors::SensorPtr _parent, sdf::ElementPtr sdf)
    {
        node_ = gazebo_ros::Node::Get(sdf);
        
        std::vector<std::vector<double>> datas;
        std::string file_name = sdf->Get<std::string>("csv_file_name");
        RCLCPP_INFO(rclcpp::get_logger("LivoxPointsPlugin"), "load csv file name: %s", file_name.c_str());
        if (!CsvReader::ReadCsvFile(file_name, datas))
        {   
            RCLCPP_INFO(rclcpp::get_logger("LivoxPointsPlugin"), "cannot get csv file! %s will return !", file_name.c_str());
            return;
        }
        sdfPtr = sdf;

        node = transport::NodePtr(new transport::Node());
        node->Init();

        // initialize for cpu/gpu ray sensor
        this->gpuRaySensor = std::dynamic_pointer_cast<sensors::GpuRaySensor>(_parent);
        this->raySensor = std::dynamic_pointer_cast<sensors::RaySensor>(_parent);
        if (this->gpuRaySensor)
        {
            RCLCPP_INFO(rclcpp::get_logger("LivoxPointsPlugin"), "LivoxPointsPlugin found gpu ray sensor\n");
            this->world = physics::get_world(this->gpuRaySensor->WorldName());
            this->offset = this->gpuRaySensor->Pose();
            gridSize = sdfPtr->Get<int>("grid");
            RCLCPP_INFO(rclcpp::get_logger("LivoxPointsPlugin"), "grid: %ld", gridSize);
        }
        else if (this->raySensor)
        {
            RCLCPP_INFO(rclcpp::get_logger("LivoxPointsPlugin"), "LivoxPointsPlugin found ray sensor\n");
            this->world = physics::get_world(this->raySensor->WorldName());
            this->offset = raySensor->Pose();
            auto rayShape = this->raySensor->LaserShape();
            this->rayShape = rayShape;
        }
        else
        {
            gzthrow("LivoxPointsPlugin requires a RaySensor or GpuRaySensor");
        }

        this->parentEntity = this->world->EntityByName(_parent->ParentName());

        // Get sensor parameters
        auto curr_scan_topic = sdf->Get<std::string>("topic");
        RCLCPP_INFO(rclcpp::get_logger("LivoxPointsPlugin"), "ros topic name: %s", curr_scan_topic.c_str());

        // PointCloud2 publisher
        cloud2_pub = node_->create_publisher<sensor_msgs::msg::PointCloud2>(curr_scan_topic + "_PointCloud2", 10);
        // CustomMsg publisher
        custom_pub = node_->create_publisher<livox_interfaces::msg::CustomMsg>(curr_scan_topic, 10);

        scanPub = node->Advertise<msgs::LaserScanStamped>(curr_scan_topic+"laserscan", 50);

        aviaInfos.clear();
        convertDataToRotateInfo(datas, aviaInfos);
        RCLCPP_INFO(rclcpp::get_logger("LivoxPointsPlugin"), "scan info size: %ld", aviaInfos.size());
        maxPointSize = aviaInfos.size();

        laserMsg.mutable_scan()->set_frame(_parent->ParentName());
        samplesStep = sdfPtr->Get<int>("samples");
        downSample = sdfPtr->Get<int>("downsample");
        minDist = sdfPtr->Get<double>("min_range");
        maxDist = sdfPtr->Get<double>("max_range");

        if (downSample < 1)
        {
            downSample = 1;
        }
        RCLCPP_INFO(rclcpp::get_logger("LivoxPointsPlugin"), "sample: %ld", samplesStep);
        RCLCPP_INFO(rclcpp::get_logger("LivoxPointsPlugin"), "downsample: %ld", downSample);

        updateRays();
        sub_ = node->Subscribe(_parent->Topic(), &LivoxPointsPlugin::OnNewLaserScans, this);
    }

    void LivoxPointsPlugin::OnNewLaserScans(const ConstLaserScanStampedPtr &_msg)
{
    // assume gpu ray sensor
    if (!rayShape && this->gpuRaySensor)
    {
        // Create laser scan message and set the timestamp
        msgs::Set(laserMsg.mutable_time(), world->SimTime());
        msgs::LaserScan *scan = laserMsg.mutable_scan();
        InitializeScan(scan);

        // Create a custom message pp_livox for publishing Livox CustomMsg type messages
        livox_interfaces::msg::CustomMsg pp_livox;
        pp_livox.header.stamp = node_->get_clock()->now();
        pp_livox.header.frame_id = gpuRaySensor->Name();
        int count = 0;
        boost::chrono::high_resolution_clock::time_point start_time = boost::chrono::high_resolution_clock::now();

        // For publishing PointCloud2 type messages
        sensor_msgs::msg::PointCloud cloud;
        cloud.header.stamp = node_->get_clock()->now();
        cloud.header.frame_id = gpuRaySensor->Name();
        auto &clouds = cloud.points;
        int ray_lindex = 0;
        // Iterate over ray scan point pairs
        for (auto &pair : points_pair)
        {
            auto rotate_info = pair.second;
            auto angle_min = this->gpuRaySensor->AngleMin().Radian();
            auto angle_max = this->gpuRaySensor->AngleMax().Radian();
            // get grid index of azimuth
            auto hindex = std::clamp((rotate_info.azimuth - angle_min) / (angle_max - angle_min) * (gridSize-1), 0.0, gridSize - 1.0);
            auto vertical_angle_min = this->gpuRaySensor->VerticalAngleMin().Radian();
            auto vertical_angle_max = this->gpuRaySensor->VerticalAngleMax().Radian();
            // get grid index of zenith
            auto vindex = std::clamp((rotate_info.zenith - vertical_angle_min) / (vertical_angle_max - vertical_angle_min) * (gridSize-1), 0.0, gridSize - 1.0);

            double range = 0;
            double intensity = 0;
            auto hlindex = (int64_t)hindex;
            auto vlindex = (int64_t)(gridSize - 1 - vindex);
            auto ray_index = hlindex + vlindex * gridSize;
            range = _msg->scan().ranges(ray_index);
            intensity = _msg->scan().intensities(ray_index);

            auto azimuth = (angle_max - angle_min) * (std::floor(hindex) / (gridSize - 1)) + angle_min;
            auto zenith = (vertical_angle_max - vertical_angle_min) * (std::floor(vindex) / (gridSize - 1)) + vertical_angle_min;
            //auto azimuth = rotate_info.azimuth;
            //auto zenith = rotate_info.zenith;

            // Handle out-of-range data
            if (range >= RangeMax())
            {
                range = 0;
            }
            else if (range <= RangeMin())
            {
                range = 0;
            }

            // Calculate point cloud data
            ignition::math::Quaterniond ray;
            // rotate
            ray.Euler(ignition::math::Vector3d(0.0, zenith, azimuth));
            auto axis = ray * ignition::math::Vector3d(1.0, 0.0, 0.0);
            auto point = range * axis;

            // Fill the CustomMsg point cloud message
            livox_interfaces::msg::CustomPoint p;
            p.x = point.X();
            p.y = point.Y();
            p.z = point.Z();
            p.reflectivity = intensity;

            // Fill the PointCloud point cloud message
            clouds.emplace_back();
            clouds.back().x = point.X();
            clouds.back().y = point.Y();
            clouds.back().z = point.Z();

            // Calculate timestamp offset
            boost::chrono::high_resolution_clock::time_point end_time = boost::chrono::high_resolution_clock::now();
            boost::chrono::nanoseconds elapsed_time = boost::chrono::duration_cast<boost::chrono::nanoseconds>(end_time - start_time);
            p.offset_time = elapsed_time.count();

            // Add point cloud data to the CustomMsg message
            pp_livox.points.push_back(p);
            count++;
        }

        if (scanPub && scanPub->HasConnections()) scanPub->Publish(laserMsg);

        // Set the number of point cloud data and publish the CustomMsg message
        pp_livox.point_num = count;
        custom_pub->publish(pp_livox);

        // Publish PointCloud2 type message
        sensor_msgs::msg::PointCloud2 cloud2;
        sensor_msgs::convertPointCloudToPointCloud2(cloud, cloud2);
        cloud2.header = cloud.header;
        cloud2_pub->publish(cloud2);
    }
    else if (rayShape) // assume cpu ray sensor
    {
        // Create laser scan message and set the timestamp
        msgs::Set(laserMsg.mutable_time(), world->SimTime());
        msgs::LaserScan *scan = laserMsg.mutable_scan();
        InitializeScan(scan);

        // Create a custom message pp_livox for publishing Livox CustomMsg type messages
        livox_interfaces::msg::CustomMsg pp_livox;
        pp_livox.header.stamp = node_->get_clock()->now();
        pp_livox.header.frame_id = raySensor->Name();
        int count = 0;
        boost::chrono::high_resolution_clock::time_point start_time = boost::chrono::high_resolution_clock::now();

        // For publishing PointCloud2 type messages
        sensor_msgs::msg::PointCloud cloud;
        cloud.header.stamp = node_->get_clock()->now();
        cloud.header.frame_id = raySensor->Name();
        auto &clouds = cloud.points;
        int ray_index = 0;
        // Iterate over ray scan point pairs
        for (auto &pair : points_pair)
        {   
            if (_msg->scan().count() <= ray_index) {
                break;
            }
            auto range = _msg->scan().ranges(ray_index);
            auto intensity = _msg->scan().intensities(ray_index);
            ray_index++;

            // Handle out-of-range data
            if (range >= RangeMax())
            {
                range = 0;
            }
            else if (range <= RangeMin())
            {
                range = 0;
            }

            // Calculate point cloud data
            auto rotate_info = pair.second;
            ignition::math::Quaterniond ray;
            ray.Euler(ignition::math::Vector3d(0.0, rotate_info.zenith, rotate_info.azimuth));
            auto axis = ray * ignition::math::Vector3d(1.0, 0.0, 0.0);
            auto point = range * axis;

            // Fill the CustomMsg point cloud message
            livox_interfaces::msg::CustomPoint p;
            p.x = point.X();
            p.y = point.Y();
            p.z = point.Z();
            p.reflectivity = intensity;

            // Fill the PointCloud point cloud message
            clouds.emplace_back();
            clouds.back().x = point.X();
            clouds.back().y = point.Y();
            clouds.back().z = point.Z();

            // Calculate timestamp offset
            boost::chrono::high_resolution_clock::time_point end_time = boost::chrono::high_resolution_clock::now();
            boost::chrono::nanoseconds elapsed_time = boost::chrono::duration_cast<boost::chrono::nanoseconds>(end_time - start_time);
            p.offset_time = elapsed_time.count();

            // Add point cloud data to the CustomMsg message
            pp_livox.points.push_back(p);
            count++;
        }

        if (scanPub && scanPub->HasConnections()) scanPub->Publish(laserMsg);

        // Set the number of point cloud data and publish the CustomMsg message
        pp_livox.point_num = count;
        custom_pub->publish(pp_livox);

        // Publish PointCloud2 type message
        sensor_msgs::msg::PointCloud2 cloud2;
        sensor_msgs::convertPointCloudToPointCloud2(cloud, cloud2);
        cloud2.header = cloud.header;
        cloud2_pub->publish(cloud2);
    }

    updateRays();
}

    void LivoxPointsPlugin::updateRays()
    {
        points_pair.clear();
        ignition::math::Vector3d start_point, end_point;
        ignition::math::Quaterniond ray;
        int64_t end_index = currStartIndex + samplesStep;
        long unsigned int ray_index = 0;
        points_pair.reserve(samplesStep);
        for (int k = currStartIndex; k < end_index; k += downSample)
        {
            auto index = k % maxPointSize;
            auto &rotate_info = aviaInfos[index];
            ray.Euler(ignition::math::Vector3d(0.0, rotate_info.zenith, rotate_info.azimuth));
            auto axis = this->offset.Rot() * ray * ignition::math::Vector3d(1.0, 0.0, 0.0);
            start_point = minDist * axis + this->offset.Pos();
            end_point = maxDist * axis + this->offset.Pos();
            if (ray_index < samplesStep)
            {
                if (this->rayShape) {
                    this->rayShape->Ray(ray_index)->SetPoints(start_point, end_point);
                }
                points_pair.emplace_back(ray_index, rotate_info);
            }
            ray_index++;
        }
        currStartIndex += samplesStep;
    }

    void LivoxPointsPlugin::InitializeScan(msgs::LaserScan *&scan)
    {
        // Store the latest laser scans into laserMsg
        msgs::Set(scan->mutable_world_pose(), this->offset + parentEntity->WorldPose());
        scan->set_angle_min(AngleMin().Radian());
        scan->set_angle_max(AngleMax().Radian());
        scan->set_angle_step(AngleResolution());
        scan->set_count(RangeCount());

        scan->set_vertical_angle_min(VerticalAngleMin().Radian());
        scan->set_vertical_angle_max(VerticalAngleMax().Radian());
        scan->set_vertical_angle_step(VerticalAngleResolution());
        scan->set_vertical_count(VerticalRangeCount());

        scan->set_range_min(RangeMin());
        scan->set_range_max(RangeMax());

        scan->clear_ranges();
        scan->clear_intensities();

        unsigned int rangeCount = RangeCount();
        unsigned int verticalRangeCount = VerticalRangeCount();

        for (unsigned int j = 0; j < verticalRangeCount; ++j)
        {
            for (unsigned int i = 0; i < rangeCount; ++i)
            {
                scan->add_ranges(0);
                scan->add_intensities(0);
            }
        }
    }

    ignition::math::Angle LivoxPointsPlugin::AngleMin() const
    {
        if (rayShape)
            return rayShape->MinAngle();
        else if (this->gpuRaySensor)
            return this->gpuRaySensor->AngleMin();
        else
            return -1;
    }

    ignition::math::Angle LivoxPointsPlugin::AngleMax() const
    {
        if (rayShape)
        {
            return ignition::math::Angle(rayShape->MaxAngle().Radian());
        }
        else if (this->gpuRaySensor)
            return this->gpuRaySensor->AngleMax();
        else
            return -1;
    }

    double LivoxPointsPlugin::GetRangeMin() const { return RangeMin(); }

    double LivoxPointsPlugin::RangeMin() const
    {
        if (rayShape)
            return rayShape->GetMinRange();
        else if (this->gpuRaySensor)
            return this->gpuRaySensor->RangeMin();
        else
            return -1;
    }

    double LivoxPointsPlugin::GetRangeMax() const { return RangeMax(); }

    double LivoxPointsPlugin::RangeMax() const
    {
        if (rayShape)
            return rayShape->GetMaxRange();
        else if (this->gpuRaySensor)
            return this->gpuRaySensor->RangeMax();
        else
            return -1;
    }

    double LivoxPointsPlugin::GetAngleResolution() const { return AngleResolution(); }

    double LivoxPointsPlugin::AngleResolution() const { return (AngleMax() - AngleMin()).Radian() / (RangeCount() - 1); }

    double LivoxPointsPlugin::GetRangeResolution() const { return RangeResolution(); }

    double LivoxPointsPlugin::RangeResolution() const
    {
        if (rayShape)
            return rayShape->GetResRange();
        else if (this->gpuRaySensor)
            return this->gpuRaySensor->RangeResolution();
        else
            return -1;
    }

    int LivoxPointsPlugin::GetRayCount() const { return RayCount(); }

    int LivoxPointsPlugin::RayCount() const
    {
        if (rayShape)
            return rayShape->GetSampleCount();
        else if (this->gpuRaySensor)
            return this->gpuRaySensor->RangeCount();
        else
            return -1;
    }

    int LivoxPointsPlugin::GetRangeCount() const { return RangeCount(); }

    int LivoxPointsPlugin::RangeCount() const
    {
        if (rayShape)
            return rayShape->GetSampleCount() * rayShape->GetScanResolution();
        else if (this->gpuRaySensor)
            return this->gpuRaySensor->RangeCount() * this->gpuRaySensor->RangeResolution();
        else
            return -1;
    }

    int LivoxPointsPlugin::GetVerticalRayCount() const { return VerticalRayCount(); }

    int LivoxPointsPlugin::VerticalRayCount() const
    {
        if (rayShape)
            return rayShape->GetVerticalSampleCount();
        else if (this->gpuRaySensor)
            return this->gpuRaySensor->VerticalRayCount();
        else
            return -1;
    }

    int LivoxPointsPlugin::GetVerticalRangeCount() const { return VerticalRangeCount(); }

    int LivoxPointsPlugin::VerticalRangeCount() const
    {
        if (rayShape)
            return rayShape->GetVerticalSampleCount() * rayShape->GetVerticalScanResolution();
        else if (this->gpuRaySensor)
            return this->gpuRaySensor->VerticalRayCount() * this->gpuRaySensor->RangeResolution();
        else
            return -1;
    }

    ignition::math::Angle LivoxPointsPlugin::VerticalAngleMin() const
    {
        if (rayShape)
        {
            return ignition::math::Angle(rayShape->VerticalMinAngle().Radian());
        }
        else if (this->gpuRaySensor)
            return -this->gpuRaySensor->VertFOV() / 2;
        else
            return -1;
    }

    ignition::math::Angle LivoxPointsPlugin::VerticalAngleMax() const
    {
        if (rayShape)
        {
            return ignition::math::Angle(rayShape->VerticalMaxAngle().Radian());
        }
        else if (this->gpuRaySensor)
            return this->gpuRaySensor->VertFOV() / 2;
        else
            return -1;
    }

    double LivoxPointsPlugin::GetVerticalAngleResolution() const { return VerticalAngleResolution(); }

    double LivoxPointsPlugin::VerticalAngleResolution() const
    {
        return (VerticalAngleMax() - VerticalAngleMin()).Radian() / (VerticalRangeCount() - 1);
    }


}
