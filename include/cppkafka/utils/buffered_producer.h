#ifndef CPPKAFKA_BUFFERED_PRODUCER_H
#define CPPKAFKA_BUFFERED_PRODUCER_H

#include <string>
#include <vector>
#include <type_traits>
#include <cstdint>
#include <unordered_set>
#include <unordered_map>
#include <map>
#include <boost/optional.hpp>
#include "../producer.h"
#include "../message.h"

namespace cppkafka {

template <typename BufferType>
class BufferedProducer {
public:
    /**
     * \brief Constructs a buffered producer using the provided configuration
     *
     * \param config The configuration to be used on the actual Producer object
     */
    BufferedProducer(Configuration config);

    /**
     * \brief Adds a message to the producer's buffer. 
     *
     * The message won't be sent until flush is called.
     *
     * \param topic The topic in which this message should be written to
     * \param partition The partition in which this message should be written to
     * \param payload The message's payload
     */
    void add_message(const std::string& topic, const Partition& partition, BufferType payload);

    /**
     * \brief Adds a message to the producer's buffer. 
     *
     * The message won't be sent until flush is called.
     *
     * \param topic The topic in which this message should be written to
     * \param partition The partition in which this message should be written to
     * \param key The message's key
     * \param payload The message's payload
     */
    void add_message(const std::string& topic, const Partition& partition, BufferType key,
                     BufferType payload);

    /**
     * \brief Flushes the buffered messages.
     *
     * This will send all messages and keep waiting until all of them are acknowledged.
     */
    void flush();

    /**
     * Gets the Producer object
     */
    Producer& get_producer();

    /**
     * Gets the Producer object
     */
    const Producer& get_producer() const;
private:
    // Pick the most appropriate index type depending on the platform we're using
    using IndexType = std::conditional<sizeof(void*) == 8, uint64_t, uint32_t>::type;

    struct BufferedMessage {
        BufferedMessage(unsigned topic_index, Partition partition, BufferType key,
                        BufferType payload) 
        : key(std::move(key)), payload(std::move(payload)), topic_index(topic_index),
          partition(partition) {

        }

        BufferedMessage(unsigned topic_index, Partition partition, BufferType payload)
        : payload(std::move(payload)), topic_index(topic_index), partition(partition) {
            
        }

        boost::optional<BufferType> key;
        BufferType payload;
        unsigned topic_index;
        Partition partition;
    };

    template <typename... Args>
    void buffer_message(const std::string& topic, Args&&... args);
    unsigned get_topic_index(const std::string& topic);
    void produce_message(IndexType index, const BufferedMessage& message);
    Configuration prepare_configuration(Configuration config);
    void on_delivery_report(const Message& message);

    Producer producer_;
    std::map<IndexType, BufferedMessage> messages_;
    std::vector<IndexType> failed_indexes_;
    IndexType current_index_{0};
    std::vector<Topic> topics_;
    std::unordered_map<std::string, unsigned> topic_mapping_;
};

template <typename BufferType>
BufferedProducer<BufferType>::BufferedProducer(Configuration config)
: producer_(prepare_configuration(std::move(config))) {

}

template <typename BufferType>
void BufferedProducer<BufferType>::add_message(const std::string& topic,
                                               const Partition& partition,
                                               BufferType payload) {
    buffer_message(topic, partition, payload);
}

template <typename BufferType>
void BufferedProducer<BufferType>::flush() {
    for (const auto& message_pair : messages_) {
        produce_message(message_pair.first, message_pair.second);
    }

    while (!messages_.empty()) {
        producer_.poll();
        if (!failed_indexes_.empty()) {
            for (const IndexType index : failed_indexes_) {
                produce_message(index, messages_.at(index));
            }
        }
        failed_indexes_.clear();
    }
}

template <typename BufferType>
Producer& BufferedProducer<BufferType>::get_producer() {
    return producer_;
}

template <typename BufferType>
const Producer& BufferedProducer<BufferType>::get_producer() const {
    return producer_;
}

template <typename BufferType>
void BufferedProducer<BufferType>::add_message(const std::string& topic,
                                               const Partition& partition,
                                               BufferType key, BufferType payload) {
    buffer_message(topic, partition, key, payload);
}

template <typename BufferType>
template <typename... Args>
void BufferedProducer<BufferType>::buffer_message(const std::string& topic, Args&&... args) {
    IndexType index = messages_.size();
    BufferedMessage message{get_topic_index(topic), std::forward<Args>(args)...};
    messages_.emplace(index, std::move(message));
}

template <typename BufferType>
unsigned BufferedProducer<BufferType>::get_topic_index(const std::string& topic) {
    auto iter = topic_mapping_.find(topic);
    if (iter == topic_mapping_.end()) {
        unsigned index = topics_.size();
        topics_.push_back(producer_.get_topic(topic));
        iter = topic_mapping_.emplace(topic, index).first;
    }
    return iter->second;
}

template <typename BufferType>
void BufferedProducer<BufferType>::produce_message(IndexType index,
                                                   const BufferedMessage& message) {
    if (message.key) {
        producer_.produce(topics_[message.topic_index], message.partition, *message.key,
                          message.payload, reinterpret_cast<void*>(index));
    }
    else {
        producer_.produce(topics_[message.topic_index], message.partition, {} /*key*/,
                          message.payload, reinterpret_cast<void*>(index));
    }
}

template <typename BufferType>
Configuration BufferedProducer<BufferType>::prepare_configuration(Configuration config) {
    using std::placeholders::_2;
    auto callback = std::bind(&BufferedProducer<BufferType>::on_delivery_report, this, _2);
    config.set_delivery_report_callback(std::move(callback));
    return config;
}

template <typename BufferType>
void BufferedProducer<BufferType>::on_delivery_report(const Message& message) {
    const IndexType index = reinterpret_cast<IndexType>(message.get_private_data());
    auto iter = messages_.find(index);
    // Got an ACK for an unexpected message?
    if (iter == messages_.end()) {
        return;
    }
    // If there was an error sending this message, then we need to re-send it
    if (message.get_error()) {
        failed_indexes_.push_back(index);
    }
    else {
        messages_.erase(iter);
    }
}

} // cppkafka

#endif // CPPKAFKA_BUFFERED_PRODUCER_H
