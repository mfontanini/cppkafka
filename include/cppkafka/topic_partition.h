#ifndef CPPKAFKA_TOPIC_PARTITION_H
#define CPPKAFKA_TOPIC_PARTITION_H

#include <string>
#include <cstdint>

namespace cppkafka {

class TopicPartition {
public:
    TopicPartition();
    TopicPartition(const char* topic);
    TopicPartition(std::string topic);
    TopicPartition(std::string topic, int partition);
    TopicPartition(std::string topic, int partition, int64_t offset);

    const std::string& get_topic() const;
    int get_partition() const;
    int64_t get_offset() const;
private:
    std::string topic_;
    int partition_;
    int64_t offset_;
};

} // cppkafka

#endif // CPPKAFKA_TOPIC_PARTITION_H
