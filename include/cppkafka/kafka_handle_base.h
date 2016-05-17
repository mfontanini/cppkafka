#ifndef CPPKAFKA_KAFKA_HANDLE_BASE_H
#define CPPKAFKA_KAFKA_HANDLE_BASE_H

#include <string>
#include <memory>
#include <librdkafka/rdkafka.h>

namespace cppkafka {

class Topic;
class TopicConfiguration;

class KafkaHandleBase {
public:
    virtual ~KafkaHandleBase() = default;

    rd_kafka_t* get_handle();
    Topic get_topic(const std::string& name);
    Topic get_topic(const std::string& name, const TopicConfiguration& config);
protected:
    KafkaHandleBase();
    KafkaHandleBase(rd_kafka_t* handle);

    void set_handle(rd_kafka_t* handle);
private:
    using HandlePtr = std::unique_ptr<rd_kafka_t, decltype(&rd_kafka_destroy)>;

    HandlePtr handle_;
};

} // cppkafka

#endif // CPPKAFKA_KAFKA_HANDLE_BASE_H
