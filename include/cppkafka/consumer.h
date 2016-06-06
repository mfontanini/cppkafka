#ifndef CPP_KAFKA_CONSUMER_H
#define CPP_KAFKA_CONSUMER_H

#include <vector>
#include <string>
#include <chrono>
#include <functional>
#include "kafka_handle_base.h"
#include "message.h"

namespace cppkafka {

class TopicConfiguration;

class Consumer : public KafkaHandleBase {
public:
    using AssignmentCallback = std::function<void(TopicPartitionList&)>;
    using RevocationCallback = std::function<void(const TopicPartitionList&)>;
    using RebalanceErrorCallback = std::function<void(rd_kafka_resp_err_t)>;

    Consumer(Configuration config);
    Consumer(const Consumer&) = delete;
    Consumer(Consumer&) = delete;
    Consumer& operator=(const Consumer&) = delete;
    Consumer& operator=(Consumer&&) = delete;
    ~Consumer();

    void set_assignment_callback(AssignmentCallback callback);
    void set_revocation_callback(RevocationCallback callback);
    void set_rebalance_error_callback(RebalanceErrorCallback callback);

    void subscribe(const std::vector<std::string>& topics);
    void unsubscribe();

    void assign(const TopicPartitionList& topic_partitions);
    void unassign();
    void close();

    void commit(const Message& msg);
    void async_commit(const Message& msg);
    void commit(const TopicPartitionList& topic_partitions);
    void async_commit(const TopicPartitionList& topic_partitions);

    OffsetTuple get_offsets(const std::string& topic, int partition) const;

    TopicPartitionList get_offsets_committed(const TopicPartitionList& topic_partitions) const;
    TopicPartitionList get_offsets_position(const TopicPartitionList& topic_partitions) const;
    TopicPartitionList get_subscription() const;
    TopicPartitionList get_assignment() const;
    std::string get_member_id() const;

    Message poll();
private:
    static void rebalance_proxy(rd_kafka_t *handle, rd_kafka_resp_err_t error,
                                rd_kafka_topic_partition_list_t *partitions, void *opaque);

    void commit(const Message& msg, bool async);
    void commit(const TopicPartitionList& topic_partitions, bool async);
    void handle_rebalance(rd_kafka_resp_err_t err, TopicPartitionList& topic_partitions);

    AssignmentCallback assignment_callback_;
    RevocationCallback revocation_callback_;
    RebalanceErrorCallback rebalance_error_callback_;
};

} // cppkafka

#endif // CPP_KAFKA_CONSUMER_H
