#include "topic.h"

using std::move;
using std::string;

namespace cppkafka {

void dummy_topic_destroyer(rd_kafka_topic_t*) {

}

Topic Topic::make_non_owning(rd_kafka_topic_t* handle) {
    return Topic(handle, NonOwningTag{});
}

Topic::Topic(rd_kafka_topic_t* handle) 
: handle_(handle, &rd_kafka_topic_destroy) {

}

Topic::Topic(rd_kafka_topic_t* handle, NonOwningTag)
: handle_(handle, &dummy_topic_destroyer) {

}

string Topic::get_name() const {
    return rd_kafka_topic_name(handle_.get());
}

rd_kafka_topic_t* Topic::get_handle() const {
    return handle_.get();
}

} // cppkafka
