#ifndef CPPKAFKA_CONFIGURATION_H
#define CPPKAFKA_CONFIGURATION_H

#include <memory>
#include <string>
#include <functional>
#include <librdkafka/rdkafka.h>
#include "topic_partition_list.h"
#include "clonable_ptr.h"

namespace cppkafka {

class Message;

class Configuration {
public:
    using DeliveryReportCallback = std::function<void(const Message&)>;
    using OffsetCommitCallback = std::function<void(rd_kafka_resp_err_t,
                                                    const TopicPartitionList& topic_partitions)>;

    Configuration();

    void set(const std::string& name, const std::string& value);
    void set_delivery_report_callback(DeliveryReportCallback callback);
    void set_offset_commit_callback(OffsetCommitCallback callback);

    rd_kafka_conf_t* get_handle() const;
    const DeliveryReportCallback& get_delivery_report_callback() const;
    const OffsetCommitCallback& get_offset_commit_callback() const;
private:
    using HandlePtr = ClonablePtr<rd_kafka_conf_t, decltype(&rd_kafka_conf_destroy),
                                  decltype(&rd_kafka_conf_dup)>;

    Configuration(rd_kafka_conf_t* ptr);
    static HandlePtr make_handle(rd_kafka_conf_t* ptr);

    HandlePtr handle_;
    DeliveryReportCallback delivery_report_callback_;
    OffsetCommitCallback offset_commit_callback_;
};

} // cppkafka

#endif // CPPKAFKA_CONFIGURATION_H
