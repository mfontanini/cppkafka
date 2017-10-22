#include <stdexcept>
#include <algorithm>
#include <cppkafka/mocking/kafka_cluster.h>
#include <cppkafka/mocking/kafka_cluster_registry.h>
#include <cppkafka/mocking/topic_partition_mock.h>

using std::shared_ptr;
using std::make_shared;
using std::string;
using std::vector;
using std::invalid_argument;
using std::runtime_error;
using std::piecewise_construct;
using std::forward_as_tuple;
using std::move;
using std::lock_guard;
using std::mutex;
using std::iota;

namespace cppkafka {
namespace mocking {

shared_ptr<KafkaCluster> KafkaCluster::make_cluster(string url) {
    shared_ptr<KafkaCluster> output{ new KafkaCluster(move(url)) };
    detail::KafkaClusterRegistry::instance().add_cluster(output);
    return output;
}

KafkaCluster::KafkaCluster(string url)
: url_(move(url)), offset_manager_(make_shared<OffsetManager>()) {

}

KafkaCluster::~KafkaCluster() {
    detail::KafkaClusterRegistry::instance().remove_cluster(*this);    
}

const string& KafkaCluster::get_url() const {
    return url_;
}

void KafkaCluster::create_topic(const string& name, unsigned partitions) {
    lock_guard<mutex> _(topics_mutex_);
    topics_.emplace(piecewise_construct, forward_as_tuple(name),
                    forward_as_tuple(name, partitions));
}

bool KafkaCluster::topic_exists(const string& name) const {
    lock_guard<mutex> _(topics_mutex_);
    return topics_.count(name) > 0;
}

void KafkaCluster::produce(const string& topic, unsigned partition, KafkaMessageMock message) {
    auto iter = topics_.find(topic);
    if (iter == topics_.end()) {
        throw invalid_argument("topic does not exist");
    }
    iter->second.add_message(partition, move(message));
}

KafkaTopicMock& KafkaCluster::get_topic(const string& name) {
    lock_guard<mutex> _(topics_mutex_);
    auto iter = topics_.find(name);
    if (iter == topics_.end()) {
        throw runtime_error("Topic " + name + " doesn't exist");
    }
    return iter->second;
}

const KafkaTopicMock& KafkaCluster::get_topic(const string& name) const {
    return const_cast<KafkaCluster&>(*this).get_topic(name);
}

void KafkaCluster::subscribe(const string& group_id, uint64_t consumer_id,
                             const vector<string>& topics,
                             AssignmentCallback assignment_callback,
                             RevocationCallback revocation_callback,
                             MessageCallback message_callback) {
    lock_guard<mutex> _(consumer_data_mutex_);
    auto iter = consumer_data_.find(consumer_id);
    // If it's already subscribed to something, unsubscribe from it
    if (iter != consumer_data_.end()) {
        do_unsubscribe(group_id, consumer_id);
    }
    ConsumerMetadata data = {
        move(assignment_callback),
        move(revocation_callback),
        move(message_callback),
    };
    iter = consumer_data_.emplace(consumer_id, move(data)).first;

    auto& group_data = group_topics_data_[group_id];
    for (const string& topic : topics) {
        group_data[topic].emplace(consumer_id);
    }
    // First revoke any assignment that involve consumers in this group
    generate_revocations(group_data);
    // Now generate the assignments
    generate_assignments(group_id, group_data);
}

void KafkaCluster::unsubscribe(const string& group_id, uint64_t consumer_id) {
    lock_guard<mutex> _(consumer_data_mutex_);
    do_unsubscribe(group_id, consumer_id);
}

void KafkaCluster::generate_assignments(const string& group_id,
                                        const TopicConsumersMap& topic_consumers) {
    for (const auto& topic_consumers_pair : topic_consumers) {
        const string& topic_name = topic_consumers_pair.first;
        const ConsumerSet& consumers = topic_consumers_pair.second;
        KafkaTopicMock& topic = get_topic(topic_name);
        vector<int> all_partitions(topic.get_partition_count());
        iota(all_partitions.begin(), all_partitions.end(), 0);

        size_t chunk_size = all_partitions.size() / consumers.size();
        size_t consumer_index = 0;
        for (const uint64_t consumer_id : consumers) {
            ConsumerMetadata& consumer = consumer_data_[consumer_id];

            // Generate partition assignment
            const size_t chunk_start = chunk_size * consumer_index;
            // For the last one, add any remainders
            if (consumer_index == consumers.size() - 1) {
                chunk_size += all_partitions.size() % consumers.size();
            }
            for (size_t i = 0; i < chunk_size; ++i) {
                consumer.partitions_assigned.emplace_back(topic_name, chunk_start + i);
            }

            // Subscribe to every assigned partition
            using namespace std::placeholders;
            for (const TopicPartitionMock& topic_partition : consumer.partitions_assigned) {
                auto& partition_mock = topic.get_partition(topic_partition.get_partition());
                auto callback = bind(consumer.message_callback, topic_partition.get_topic(),
                                     topic_partition.get_partition(), _1, _2);
                const auto subscriber_id = partition_mock.subscribe(move(callback));
                consumer.subscriptions[&topic].emplace(topic_partition.get_partition(),
                                                       subscriber_id);
            }
            // Now trigger the assignment callback
            consumer_index++;
        }
    }
    // Now do another pass and trigger the assignment callbacks
    for (const auto& topic_consumers_pair : topic_consumers) {
        for (const uint64_t consumer_id : topic_consumers_pair.second) {
            ConsumerMetadata& consumer = consumer_data_[consumer_id];
            // Try to load the offsets for this consumer,  and store the updated offsets and
            // trigger the assignment callback
            auto partitions_assigned = move(consumer.partitions_assigned);
            consumer.partitions_assigned = offset_manager_->get_offsets(group_id,
                                                                        move(partitions_assigned));
            consumer.assignment_callback(consumer.partitions_assigned);
        }
    }
}

void KafkaCluster::generate_revocations(const TopicConsumersMap& topic_consumers) {
    for (const auto& topic_consumers_pair : topic_consumers) {
        const ConsumerSet& consumers = topic_consumers_pair.second;
        for (const uint64_t consumer_id : consumers) {
            ConsumerMetadata& consumer = consumer_data_[consumer_id];
            // Execute revocation callback and unsubscribe from the partition object
            if (!consumer.partitions_assigned.empty()) {
                consumer.revocation_callback(consumer.partitions_assigned);
                for (const auto& topic_subscription : consumer.subscriptions) {
                    KafkaTopicMock& topic = *topic_subscription.first;
                    for (const auto& partition_subscription : topic_subscription.second) {
                        auto& partition_mock = topic.get_partition(partition_subscription.first);
                        partition_mock.unsubscribe(partition_subscription.second);
                    }
                }
                consumer.partitions_assigned.clear();
            }
            consumer.subscriptions.clear();
        }
    }
}

void KafkaCluster::do_unsubscribe(const string& group_id, uint64_t consumer_id) {
    auto iter = consumer_data_.find(consumer_id);
    if (iter == consumer_data_.end()) {
        return;
    }
    auto& group_data = group_topics_data_[group_id];
    // Revoke for all consumers
    generate_revocations(group_data);

    for (const auto& topic_subscription : iter->second.subscriptions) {
        const string& topic_name = topic_subscription.first->get_name();
        auto& topic_data = group_data[topic_name];
        topic_data.erase(consumer_id);
        if (topic_data.empty()) {
            group_data.erase(topic_name);
        }
    }
    if (group_data.empty()) {
        // If we ran out of consumers for this group, erase it
        group_topics_data_.erase(group_id);
    }
    else {
        // Otherwise re-generate the assignments
        generate_assignments(group_id, group_data);
    }
}

} // mocking
} // cppkafka
