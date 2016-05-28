#include <gtest/gtest.h>
#include "cppkafka/topic_partition_list.h"
#include "cppkafka/topic_partition.h"

using namespace cppkafka;

class TopicPartitionListTest : public testing::Test {
public:
    
};

TEST_F(TopicPartitionListTest, Conversion) {
    TopicPartitionList list1;
    list1.push_back("foo");
    list1.push_back({ "bar", 2 });

    TopicPartitionList list2 = convert(convert(list1));

    EXPECT_EQ(list1.size(), list2.size());
    for (size_t i = 0; i < list1.size(); ++i) {
        const auto& item1 = list1[i];
        const auto& item2 = list2[i];
        EXPECT_EQ(item1.get_topic(), item2.get_topic());
        EXPECT_EQ(item1.get_partition(), item2.get_partition());
        EXPECT_EQ(item1.get_offset(), item2.get_offset());
    }
}
