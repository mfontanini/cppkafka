#ifndef CPPKAFKA_TOPIC_CONFIGURATION_H
#define CPPKAFKA_TOPIC_CONFIGURATION_H

#include <memory>
#include <string>
#include <librdkafka/rdkafka.h>

namespace cppkafka {

class TopicConfiguration {
public:
    TopicConfiguration();
    TopicConfiguration(const TopicConfiguration& rhs);
    TopicConfiguration(TopicConfiguration&& rhs) noexcept = default;
    TopicConfiguration& operator=(const TopicConfiguration& rhs);
    TopicConfiguration& operator=(TopicConfiguration&& rhs) noexcept = default;

    void set(const std::string& name, const std::string& value);

    rd_kafka_topic_conf_t* get_handle() const;
private:
    using HandlePtr = std::unique_ptr<rd_kafka_topic_conf_t,
                                      decltype(&rd_kafka_topic_conf_destroy)>;

    TopicConfiguration(rd_kafka_topic_conf_t* ptr);
    static HandlePtr make_handle(rd_kafka_topic_conf_t* ptr);

    HandlePtr handle_;
};

} // cppkafka

#endif // CPPKAFKA_TOPIC_CONFIGURATION_H
