#ifndef CPPKAFKA_TOPIC_CONFIGURATION_H
#define CPPKAFKA_TOPIC_CONFIGURATION_H

#include <string>
#include <functional>
#include <librdkafka/rdkafka.h>
#include "clonable_ptr.h"
#include "configuration_base.h"

namespace cppkafka {

class Topic;
class Buffer;

class TopicConfiguration : public ConfigurationBase<TopicConfiguration> {
public:
    using PartitionerCallback = std::function<int32_t(const Topic&, const Buffer& key,
                                                      int32_t partition_count)>;

    using ConfigurationBase<TopicConfiguration>::set;

    TopicConfiguration();

    void set(const std::string& name, const std::string& value);

    void set_partitioner_callback(PartitionerCallback callback);

    void set_as_opaque();

    const PartitionerCallback& get_partitioner_callback() const;
    rd_kafka_topic_conf_t* get_handle() const;
    std::string get(const std::string& name) const;
private:
    using HandlePtr = ClonablePtr<rd_kafka_topic_conf_t,
                                  decltype(&rd_kafka_topic_conf_destroy),
                                  decltype(&rd_kafka_topic_conf_dup)>;

    TopicConfiguration(rd_kafka_topic_conf_t* ptr);
    static HandlePtr make_handle(rd_kafka_topic_conf_t* ptr);

    HandlePtr handle_;
    PartitionerCallback partitioner_callback_;
};

} // cppkafka

#endif // CPPKAFKA_TOPIC_CONFIGURATION_H
