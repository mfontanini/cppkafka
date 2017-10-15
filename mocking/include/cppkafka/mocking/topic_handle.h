#ifndef CPPKAFKA_MOCKING_TOPIC_HANDLE_H
#define CPPKAFKA_MOCKING_TOPIC_HANDLE_H

#include <string>

namespace cppkafka {
namespace mocking {

class TopicHandle {
public:
    TopicHandle(std::string topic, void* opaque)
    : topic_(move(topic)), opaque_(opaque) {

    }

    const std::string& get_topic() const {
        return topic_;
    }

    void* get_opaque() const {
        return opaque_;
    }
private:
    const std::string topic_;
    void* opaque_;
};

} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_TOPIC_HANDLE_H
