#ifndef CPPKAFKA_PRODUCER_H
#define CPPKAFKA_PRODUCER_H

#include <memory>
#include "kafka_handle_base.h"
#include "configuration.h"
#include "buffer.h"
#include "topic.h"
#include "partition.h"

namespace cppkafka {

class Topic;
class Buffer;
class Partition;
class TopicConfiguration;

class Producer : public KafkaHandleBase {
public:
    enum PayloadPolicy {
        COPY_PAYLOAD = RD_KAFKA_MSG_F_COPY, ///< Means RD_KAFKA_MSG_F_COPY
        FREE_PAYLOAD = RD_KAFKA_MSG_F_FREE  ///< Means RD_KAFKA_MSG_F_FREE
    };

    Producer(Configuration config);

    void set_payload_policy(PayloadPolicy policy);
    PayloadPolicy get_payload_policy() const;

    const Configuration& get_configuration() const;

    void produce(const Topic& topic, const Partition& partition, const Buffer& payload);
    void produce(const Topic& topic, const Partition& partition, const Buffer& payload,
                 const Buffer& key);
    void produce(const Topic& topic, const Partition& partition, const Buffer& payload,
                 const Buffer& key, void* user_data);

    int poll();
private:
    Configuration config_;
    PayloadPolicy message_payload_policy_;
};

} // cppkafka

#endif // CPPKAFKA_PRODUCER_H
