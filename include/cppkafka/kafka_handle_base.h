#ifndef CPPKAFKA_KAFKA_HANDLE_BASE_H
#define CPPKAFKA_KAFKA_HANDLE_BASE_H

#include <string>
#include <memory>
#include <chrono>
#include <librdkafka/rdkafka.h>
#include "topic_partition_list.h"
#include "metadata.h"

namespace cppkafka {

class Topic;
class TopicConfiguration;

class KafkaHandleBase {
public:
    virtual ~KafkaHandleBase() = default;
    KafkaHandleBase(const KafkaHandleBase&) = delete;
    KafkaHandleBase(KafkaHandleBase&&) = delete;
    KafkaHandleBase& operator=(const KafkaHandleBase&) = delete;
    KafkaHandleBase& operator=(KafkaHandleBase&&) = delete;

    void pause_partitions(const TopicPartitionList& topic_partitions);
    void resume_partitions(const TopicPartitionList& topic_partitions);

    void set_timeout(const std::chrono::milliseconds& timeout);

    rd_kafka_t* get_handle();
    Topic get_topic(const std::string& name);
    Topic get_topic(const std::string& name, TopicConfiguration config);
    Metadata get_metadata();
    Metadata get_metadata(const Topic& topic);
    std::chrono::milliseconds get_timeout() const;
protected:
    KafkaHandleBase();
    KafkaHandleBase(rd_kafka_t* handle);

    void set_handle(rd_kafka_t* handle);
    void check_error(rd_kafka_resp_err_t error);
private:
    static const std::chrono::milliseconds DEFAULT_TIMEOUT;

    using HandlePtr = std::unique_ptr<rd_kafka_t, decltype(&rd_kafka_destroy)>;

    Topic get_topic(const std::string& name, rd_kafka_topic_conf_t* conf);
    Metadata get_metadata(rd_kafka_topic_t* topic_ptr);

    HandlePtr handle_;
    std::chrono::milliseconds timeout_ms_;
};

} // cppkafka

#endif // CPPKAFKA_KAFKA_HANDLE_BASE_H
