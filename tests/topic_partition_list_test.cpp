#include <sstream>
#include <gtest/gtest.h>
#include "cppkafka/topic_partition_list.h"
#include "cppkafka/topic_partition.h"

using std::ostringstream;

using namespace cppkafka;

class TopicPartitionListTest : public testing::Test {
public:
    
};

TEST_F(TopicPartitionListTest, Conversion) {
    TopicPartitionList list1;
    list1.push_back("foo");
    list1.push_back({ "bar", 2 });
    list1.push_back({ "asd", 2, TopicPartition::OFFSET_BEGINNING });

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

TEST_F(TopicPartitionListTest, AsString) {
    ostringstream output;
    TopicPartition topic_partition("foo", 5);
    output << topic_partition;
    EXPECT_EQ("foo[5]", output.str());
}

TEST_F(TopicPartitionListTest, ListAsString) {
    ostringstream output;
    TopicPartitionList list;
    list.push_back("foo");
    list.push_back({ "bar", 2 });

    output << list;
    EXPECT_EQ("[ foo[-1], bar[2] ]", output.str());
}