#ifndef CPPKAFKA_PRODUCER_H
#define CPPKAFKA_PRODUCER_H

#include <memory>
#include "kafka_handle_base.h"
#include "configuration.h"

namespace cppkafka {

class Topic;
class Buffer;
class Partition;

class Producer : public KafkaHandleBase {
public:
    Producer(Configuration config);

    void produce(const Topic& topic, const Partition& partition, const Buffer& payload);
    void produce(const Topic& topic, const Partition& partition, const Buffer& payload,
                 const Buffer& key);
    void produce(const Topic& topic, const Partition& partition, const Buffer& payload,
                 const Buffer& key, void* user_data);
private:
    Configuration config_;
    int message_payload_policy_;
};

} // cppkafka

#endif // CPPKAFKA_PRODUCER_H
