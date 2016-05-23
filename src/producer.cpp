#include <errno.h>
#include "producer.h"
#include "exceptions.h"

using std::move;
using std::string;

namespace cppkafka {

Producer::Producer(Configuration config)
: config_(move(config)) {
    char error_buffer[512];
    rd_kafka_t* ptr = rd_kafka_new(RD_KAFKA_PRODUCER,
                                   rd_kafka_conf_dup(config_.get_handle()),
                                   error_buffer, sizeof(error_buffer));
    if (!ptr) {
        throw Exception("Failed to create producer handle: " + string(error_buffer));
    }
    rd_kafka_set_log_level(ptr, 7);
    set_handle(ptr);
    set_payload_policy(Producer::COPY_PAYLOAD);
}

void Producer::set_payload_policy(PayloadPolicy policy) {
    message_payload_policy_ = policy;
}

Producer::PayloadPolicy Producer::get_payload_policy() const {
    return message_payload_policy_;
}

void Producer::produce(const Topic& topic, const Partition& partition, const Buffer& payload) {
    produce(topic, partition, payload, Buffer{} /*key*/, nullptr /*user_data*/);
}

void Producer::produce(const Topic& topic, const Partition& partition, const Buffer& payload,
                       const Buffer& key) {
    produce(topic, partition, payload, key, nullptr /*user_data*/);
}

void Producer::produce(const Topic& topic, const Partition& partition, const Buffer& payload,
                       const Buffer& key, void* user_data) {
    void* payload_ptr = (void*)payload.get_data(); 
    void* key_ptr = (void*)key.get_data(); 
    const int policy = static_cast<int>(message_payload_policy_);
    int result = rd_kafka_produce(topic.get_handle(), partition.get_partition(),
                                  policy, payload_ptr, payload.get_size(),
                                  key_ptr, key.get_size(), user_data);
    if (result == -1) {
        throw HandleException(rd_kafka_errno2err(errno));
    }
}

} // cppkafka
