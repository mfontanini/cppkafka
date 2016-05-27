#ifndef CPPKAFKA_METADATA_H
#define CPPKAFKA_METADATA_H

#include <memory>
#include <string>
#include <vector>
#include <cstdint>
#include <unordered_set>
#include <librdkafka/rdkafka.h>

namespace cppkafka {

class PartitionMetadata {
public:
    PartitionMetadata(const rd_kafka_metadata_partition& partition);

    uint32_t get_id() const;
    rd_kafka_resp_err_t get_error() const;
    int32_t get_leader() const;
    const std::vector<int32_t>& get_replicas() const;
    const std::vector<int32_t>& get_in_sync_replica_brokers() const;
private:
    int32_t id_;
    rd_kafka_resp_err_t error_;
    int32_t leader_;
    std::vector<int32_t> replicas_;
    std::vector<int32_t> isrs_;
};

class TopicMetadata {
public:
    TopicMetadata(const rd_kafka_metadata_topic& topic);

    const std::string& get_topic() const;
    rd_kafka_resp_err_t get_error() const;
    const std::vector<PartitionMetadata>& get_partitions() const;
private:
    std::string topic_;
    rd_kafka_resp_err_t error_;
    std::vector<PartitionMetadata> partitions_;
};

class BrokerMetadata {
public:
    BrokerMetadata(const rd_kafka_metadata_broker_t& broker);

    const std::string& get_host() const;
    int32_t get_id() const;
    uint16_t get_port() const;
private:
    const std::string host_;
    int32_t id_;
    uint16_t port_;
};

class Metadata {
public:
    Metadata(const rd_kafka_metadata_t* ptr);

    std::vector<BrokerMetadata> get_brokers() const;
    std::vector<TopicMetadata> get_topics() const;
    std::vector<TopicMetadata> get_topics(const std::unordered_set<std::string>& topics) const;
    std::vector<TopicMetadata> get_topics(const std::string& prefix) const;
private:
    using HandlePtr = std::unique_ptr<const rd_kafka_metadata_t, decltype(&rd_kafka_metadata_destroy)>;

    HandlePtr handle_;
};

} // cppkafka

#endif // CPPKAFKA_METADATA_H
