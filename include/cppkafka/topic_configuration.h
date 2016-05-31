#ifndef CPPKAFKA_TOPIC_CONFIGURATION_H
#define CPPKAFKA_TOPIC_CONFIGURATION_H

#include <string>
#include <librdkafka/rdkafka.h>
#include "clonable_ptr.h"

namespace cppkafka {

class TopicConfiguration {
public:
    TopicConfiguration();

    void set(const std::string& name, const std::string& value);

    rd_kafka_topic_conf_t* get_handle() const;
private:
    using HandlePtr = ClonablePtr<rd_kafka_topic_conf_t, decltype(&rd_kafka_topic_conf_destroy),
                                  decltype(&rd_kafka_topic_conf_dup)>;

    TopicConfiguration(rd_kafka_topic_conf_t* ptr);
    static HandlePtr make_handle(rd_kafka_topic_conf_t* ptr);

    HandlePtr handle_;
};

} // cppkafka

#endif // CPPKAFKA_TOPIC_CONFIGURATION_H
