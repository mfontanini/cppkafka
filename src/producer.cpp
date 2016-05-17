#include <errno.h>
#include "producer.h"
#include "exceptions.h"
#include "buffer.h"
#include "topic.h"
#include "partition.h"

using std::move;
using std::string;

namespace cppkafka {

Producer::Producer(Configuration config)
: config_(move(config)), message_payload_policy_(RD_KAFKA_MSG_F_COPY) {
    char error_buffer[512];
    rd_kafka_t* ptr = rd_kafka_new(RD_KAFKA_PRODUCER, config_.get_handle(),
                                   error_buffer, sizeof(error_buffer));
    if (!ptr) {
        throw Exception("Failed to create producer handle: " + string(error_buffer));
    }
    set_handle(ptr);
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
    int result = rd_kafka_produce(topic.get_handle(), partition.get_partition(),
                                  message_payload_policy_, payload_ptr, payload.get_size(),
                                  key_ptr, key.get_size(), user_data);
    if (result == -1) {
        throw HandleException(rd_kafka_errno2err(errno));
    }
}

} // cppkafka
