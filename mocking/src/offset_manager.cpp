#include <cppkafka/mocking/offset_manager.h>
#include <cppkafka/mocking/topic_partition_mock.h>

using std::string;
using std::vector;
using std::mutex;
using std::lock_guard;
using std::make_tuple;

namespace cppkafka {
namespace mocking {

void OffsetManager::commit_offsets(const string& group_id,
                                   const vector<TopicPartitionMock>& topic_partitions) {
    lock_guard<mutex> _(offsets_mutex_);
    for (const TopicPartitionMock& topic_partition : topic_partitions) {
        auto key = make_tuple(group_id, topic_partition.get_topic(),
                              topic_partition.get_partition()); 
        auto iter = offsets_.find(key);
        if (iter == offsets_.end()) {
            offsets_.emplace(key, topic_partition.get_offset()).first;
        }
        else {
            iter->second = topic_partition.get_offset();
        }
    }
}

vector<TopicPartitionMock>
OffsetManager::get_offsets(const string& group_id,
                           vector<TopicPartitionMock> topic_partitions) const {
    lock_guard<mutex> _(offsets_mutex_);
    for (TopicPartitionMock& topic_partition : topic_partitions) {
        if (topic_partition.get_offset() != RD_KAFKA_OFFSET_INVALID) {
            auto iter = offsets_.find(make_tuple(group_id, topic_partition.get_topic(),
                                                 topic_partition.get_partition()));
            if (iter != offsets_.end()) {
                topic_partition.set_offset(iter->second);
            }
        }
    }
    return topic_partitions;
}

} // mocking
} // cppkafka
