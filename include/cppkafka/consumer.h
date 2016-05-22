#ifndef CPP_KAFKA_CONSUMER_H
#define CPP_KAFKA_CONSUMER_H

#include <vector>
#include <string>
#include "kafka_handle_base.h"
#include "topic_partition_list.h"

namespace cppkafka {

class Configuration;
class TopicConfiguration;

class Consumer : public KafkaHandleBase {
public:
    Consumer(const Configuration& config);

    void subscribe(const std::vector<std::string>& topics);
    void unsubscribe();

    void assign(const TopicPartitionList& topic_partitions);
private:
    void check_error(rd_kafka_resp_err_t error);
};

} // cppkafka

#endif // CPP_KAFKA_CONSUMER_H
