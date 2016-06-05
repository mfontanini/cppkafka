#ifndef CPPKAFKA_CONFIGURATION_H
#define CPPKAFKA_CONFIGURATION_H

#include <memory>
#include <string>
#include <functional>
#include <chrono>
#include <librdkafka/rdkafka.h>
#include "topic_partition_list.h"
#include "clonable_ptr.h"

namespace cppkafka {

class Message;
class Producer;
class Consumer;
class KafkaHandleBase;

class Configuration {
public:
    using DeliveryReportCallback = std::function<void(Producer& producer, const Message&)>;
    using OffsetCommitCallback = std::function<void(Consumer& consumer, rd_kafka_resp_err_t,
                                                    const TopicPartitionList& topic_partitions)>;
    using ErrorCallback = std::function<void(KafkaHandleBase& handle, int error,
                                             const std::string& reason)>;
    using ThrottleCallback = std::function<void(KafkaHandleBase& handle,
                                                const std::string& broker_name,
                                                int32_t broker_id,
                                                std::chrono::milliseconds throttle_time)>;
    using LogCallback = std::function<void(KafkaHandleBase& handle, int level,
                                           const std::string& facility,
                                           const std::string& message)>;
    using StatsCallback = std::function<void(KafkaHandleBase& handle, const std::string& json)>;
    using SocketCallback = std::function<int(int domain, int type, int protoco)>;

    Configuration();

    void set(const std::string& name, const std::string& value);
    void set_delivery_report_callback(DeliveryReportCallback callback);
    void set_offset_commit_callback(OffsetCommitCallback callback);
    void set_error_callback(ErrorCallback callback);
    void set_throttle_callback(ThrottleCallback callback);
    void set_log_callback(LogCallback callback);
    void set_stats_callback(StatsCallback callback);
    void set_socket_callback(SocketCallback callback);

    rd_kafka_conf_t* get_handle() const;
    const DeliveryReportCallback& get_delivery_report_callback() const;
    const OffsetCommitCallback& get_offset_commit_callback() const;
    const ErrorCallback& get_error_callback() const;
    const ThrottleCallback& get_throttle_callback() const;
    const LogCallback& get_log_callback() const;
    const StatsCallback& get_stats_callback() const;
    const SocketCallback& get_socket_callback() const;
private:
    using HandlePtr = ClonablePtr<rd_kafka_conf_t, decltype(&rd_kafka_conf_destroy),
                                  decltype(&rd_kafka_conf_dup)>;

    Configuration(rd_kafka_conf_t* ptr);
    static HandlePtr make_handle(rd_kafka_conf_t* ptr);

    HandlePtr handle_;
    DeliveryReportCallback delivery_report_callback_;
    OffsetCommitCallback offset_commit_callback_;
    ErrorCallback error_callback_;
    ThrottleCallback throttle_callback_;
    LogCallback log_callback_;
    StatsCallback stats_callback_;
    SocketCallback socket_callback_;
};

} // cppkafka

#endif // CPPKAFKA_CONFIGURATION_H
