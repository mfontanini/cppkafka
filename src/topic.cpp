#include "topic.h"

using std::move;
using std::string;

namespace cppkafka {

Topic::Topic(rd_kafka_topic_t* handle) 
: handle_(handle, &rd_kafka_topic_destroy) {

}

string Topic::get_name() const {
    return rd_kafka_topic_name(handle_.get());
}

rd_kafka_topic_t* Topic::get_handle() const {
    return handle_.get();
}

} // cppkafka
