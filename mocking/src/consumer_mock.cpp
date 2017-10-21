#include <atomic>
#include <stdexcept>
#include <cppkafka/mocking/consumer_mock.h>
#include <cppkafka/mocking/kafka_cluster.h>

using std::atomic;
using std::vector;
using std::string;
using std::move;
using std::bind;
using std::runtime_error;

namespace cppkafka {
namespace mocking {

static const string CONFIG_GROUP_ID = "group.id";

uint64_t ConsumerMock::make_consumer_id() {
    static atomic<uint64_t> current_id{0};
    return current_id++;
}

ConsumerMock::ConsumerMock(ConfigurationMock config, EventProcessorPtr processor,
                           ClusterPtr cluster)
: HandleMock(move(processor), move(cluster)), config_(move(config)),
  consumer_id_(make_consumer_id()) {
    if (!config_.has_key(CONFIG_GROUP_ID)) {
        throw runtime_error("Failed to find " + CONFIG_GROUP_ID + " in config");
    }
}

ConsumerMock::~ConsumerMock() {
    auto& cluster = get_cluster();
    for (const string& topic_name : subscribed_topics_) {
        cluster.acquire_topic(topic_name, [&](KafkaTopicMock& topic) {
            topic.unsubscribe(group_id_, consumer_id_);
        });
    }
}

void ConsumerMock::subscribe(const vector<string>& topics) {
    using std::placeholders::_1;
    auto& cluster = get_cluster();
    for (const string& topic_name : topics) {
        if (subscribed_topics_.count(topic_name) > 0) {
            continue;
        }
        cluster.acquire_topic(topic_name, [&](KafkaTopicMock& topic) {
            topic.subscribe(
                group_id_,
                consumer_id_,
                bind(&ConsumerMock::on_assignment, this, _1),
                bind(&ConsumerMock::on_revocation, this, _1),
                bind(&ConsumerMock::on_message, this, _1)
            );
        });
    }
}

void ConsumerMock::on_assignment(vector<TopicPartitionMock>& topic_partitions) {

}

void ConsumerMock::on_revocation(const vector<TopicPartitionMock>& topic_partitions) {

}

void ConsumerMock::on_message(uint64_t offset) {

}

} // mocking
} // cppkafka
