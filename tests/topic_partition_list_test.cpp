#include <sstream>
#include <catch.hpp>
#include "cppkafka/topic_partition_list.h"
#include "cppkafka/topic_partition.h"

using std::ostringstream;
using std::set;
using std::string;

using namespace cppkafka;

TEST_CASE("rdkafka conversion", "[topic_partition]") {
    TopicPartitionList list1;
    list1.push_back("foo");
    list1.push_back({ "bar", 2 });
    list1.push_back({ "asd", 2, TopicPartition::OFFSET_BEGINNING });

    TopicPartitionList list2 = convert(convert(list1));

    CHECK(list1.size() == list2.size());
    for (size_t i = 0; i < list1.size(); ++i) {
        const auto& item1 = list1[i];
        const auto& item2 = list2[i];
        CHECK(item1.get_topic() == item2.get_topic());
        CHECK(item1.get_partition() == item2.get_partition());
        CHECK(item1.get_offset() == item2.get_offset());
    }
}

TEST_CASE("topic partition to string", "[topic_partition]") {
    ostringstream output;
    TopicPartition topic_partition("foo", 5);
    output << topic_partition;
    CHECK(output.str() == "foo[5:#]");
}

TEST_CASE("topic partition list to string", "[topic_partition]") {
    ostringstream output;
    TopicPartitionList list;
    list.push_back("foo");
    list.push_back({ "bar", 2 });
    list.push_back({ "foobar", 3, 4 });

    output << list;
    CHECK(output.str() == "[ foo[-1:#], bar[2:#], foobar[3:4] ]");
}

TEST_CASE("find matches by topic", "[topic_partition]") {
    TopicPartitionList list;
    list.push_back({ "foo", 0 });
    list.push_back({ "bar", 3 });
    list.push_back({ "fb",  1 });
    list.push_back({ "foo", 1 });
    list.push_back({ "fb",  2 });
    list.push_back({ "other", 1 });
    list.push_back({ "a",   1 });

    TopicPartitionList subset = find_matches(list, set<string>{"foo", "fb"});
    REQUIRE(subset.size() == 4);
    CHECK((subset[0].get_topic() == "foo" && subset[0].get_partition() == 0));
    CHECK((subset[1].get_topic() == "fb" && subset[1].get_partition() == 1));
    CHECK((subset[2].get_topic() == "foo" && subset[2].get_partition() == 1));
    CHECK((subset[3].get_topic() == "fb" && subset[3].get_partition() == 2));
}

TEST_CASE("find matches by id", "[topic_partition]") {
    TopicPartitionList list;
    list.push_back({ "foo", 2 });
    list.push_back({ "foo", 3 });
    list.push_back({ "foo", 4 });
    list.push_back({ "foo", 5 });
    list.push_back({ "foo", 6 });
    list.push_back({ "foo", 7 });
    list.push_back({ "foo", 8 });

    TopicPartitionList subset = find_matches(list, set<int>{2,5,8});
    REQUIRE(subset.size() == 3);
    CHECK(subset[0].get_partition() == 2);
    CHECK(subset[1].get_partition() == 5);
    CHECK(subset[2].get_partition() == 8);
}
