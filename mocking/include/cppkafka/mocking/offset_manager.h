#ifndef CPPKAFKA_MOCKING_OFFSET_MANAGER_H
#define CPPKAFKA_MOCKING_OFFSET_MANAGER_H

#include <map>
#include <string>
#include <tuple>
#include <cstdint>
#include <mutex>
#include <vector>

namespace cppkafka {
namespace mocking {

class TopicPartitionMock;

class OffsetManager {
public:
    void commit_offsets(const std::string& group_id, const std::vector<TopicPartitionMock>&);
    std::vector<TopicPartitionMock> get_offsets(const std::string& group_id,
                                                std::vector<TopicPartitionMock>) const;
private:
    // (consumer, topic, partition)
    using OffsetIdentifier = std::tuple<std::string, std::string, int>;
    using OffsetMap = std::map<OffsetIdentifier, int64_t>;

    OffsetMap offsets_;
    mutable std::mutex offsets_mutex_;
};

} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_OFFSET_MANAGER_H

