#include <vector>
#include <thread>
#include <set>
#include <mutex>
#include <chrono>
#include <iterator>
#include <condition_variable>
#include <catch.hpp>
#include "cppkafka/consumer.h"
#include "cppkafka/producer.h"
#include "cppkafka/header_list.h"
#include "test_utils.h"

using std::vector;
using std::move;
using std::string;
using std::thread;
using std::set;
using std::mutex;
using std::tie;
using std::condition_variable;
using std::lock_guard;
using std::unique_lock;
using std::make_move_iterator;
using std::chrono::seconds;
using std::chrono::milliseconds;
using std::chrono::system_clock;

#if (RD_KAFKA_VERSION >= RD_KAFKA_HEADERS_SUPPORT_VERSION)

using namespace cppkafka;
using StringHeader = Header<std::string>;
using BufferHeader = Header<Buffer>;

TEST_CASE("creation", "[headers]") {
    SECTION("empty") {
        HeaderList<StringHeader> list;
        REQUIRE(!!list == false);
    }
    
    SECTION("default") {
        HeaderList<StringHeader> list(2);
        REQUIRE(!!list == true);
        REQUIRE(list.size() == 0);
        REQUIRE(list.empty() == true);
        REQUIRE(list.get_handle() != nullptr);
    }
    
    SECTION("from handle") {
        HeaderList<StringHeader> list(rd_kafka_headers_new(1));
        REQUIRE(!!list == true);
        REQUIRE(list.size() == 0);
        REQUIRE(list.empty() == true);
        REQUIRE(list.get_handle() != nullptr);
    }
}

TEST_CASE("release", "[headers]") {
    HeaderList<StringHeader> list(2);
    auto handle = list.release_handle();
    REQUIRE(handle != nullptr);
    REQUIRE(list.release_handle() == nullptr); //release again
    REQUIRE(!!list == false);
    rd_kafka_headers_destroy(handle);
}

TEST_CASE("modify", "[headers]") {
    SECTION("add") {
        HeaderList<StringHeader> list(10);
        //empty header name
        list.add({{}, "payload1"});
        //empty payload
        list.add({"header2", {}});
        list.add({"header3", "payload3"});
        //both null
        list.add({{}, {}});
        //both empty (0-length strings)
        list.add({"", ""});
        
        //validate
        REQUIRE(list.size() == 5);
        REQUIRE_FALSE(list.empty());
        
        //access a header
        REQUIRE(list.at(1).get_name() == "header2");
        REQUIRE(list.at(1).get_value().empty());
        REQUIRE(list.at(2).get_value() == "payload3");
    }
    
    SECTION("remove") {
        HeaderList<StringHeader> list(10);
        //empty header name
        list.add({{}, "payload1"});
        //empty payload
        list.add({"header2", {}});
        list.add({"header3", "payload3"});
        //both null
        list.add({{}, {}});
        //both empty (0 length strings)
        list.add({"", ""});
        
        //Remove a bogus name
        Error err = list.remove("bogus");
        REQUIRE(err.get_error() == RD_KAFKA_RESP_ERR__NOENT);
        //Remove header with name
        list.remove("header2");
        REQUIRE(list.size() == 4);
        list.remove("header3");
        REQUIRE(list.size() == 3);
        //Remove headers without name
        list.remove({});
        REQUIRE(list.size() == 0);
    }
}

TEST_CASE("copy and move", "[headers]") {
    SECTION("copy owning") {
        //Create an owning header list and copy it
        HeaderList<StringHeader> list(3), list2(3);
        list.add({"header1", "payload1"});
        list.add({"header2", "payload2"});
        list.add({"header3", "payload3"});
        REQUIRE(list2.size() == 0);
        list2 = list;
        REQUIRE(list2.size() == 3);
        REQUIRE(list2.size() == list.size());
        //make sure the handles are different
        CHECK(list.get_handle() != list2.get_handle());
        CHECK(list.at(0) == list2.at(0));
        CHECK(list.at(1) == list2.at(1));
        CHECK(list.at(2) == list2.at(2));
        CHECK(list == list2);
    }
    
    SECTION("copy owning with buffers") {
        //Create an owning header list and copy it
        HeaderList<BufferHeader> list(3), list2(3);
        string payload1 = "payload1", payload2 = "payload2", payload3 = "payload3";
        list.add({"header1", payload1});
        list.add({"header2", payload2});
        list.add({"header3", payload3});
        REQUIRE(list2.size() == 0);
        list2 = list;
        REQUIRE(list2.size() == 3);
        REQUIRE(list2.size() == list.size());
        //make sure the handles are different
        CHECK(list.get_handle() != list2.get_handle());
        CHECK(list.at(0) == list2.at(0));
        CHECK(list.at(1) == list2.at(1));
        CHECK(list.at(2) == list2.at(2));
        CHECK(list == list2);
    }
    
    SECTION("copy non-owning") {
        //Create an owning header list and copy it
        HeaderList<StringHeader> list(3), list2(3), list3(HeaderList<StringHeader>::make_non_owning(list.get_handle()));
        list.add({"header1", "payload1"});
        list.add({"header2", "payload2"});
        list.add({"header3", "payload3"});
        list2 = list3; //copy non-owning list
        REQUIRE(list.size() == 3);
        REQUIRE(list3.size() == list.size());
        REQUIRE(list2.size() == list.size());
        //make sure the handles are the same
        CHECK(list2.get_handle() == list3.get_handle());
        CHECK(list2.at(0) == list3.at(0));
        CHECK(list2.at(1) == list3.at(1));
        CHECK(list2.at(2) == list3.at(2));
        CHECK(list2 == list3);
    }
    
    SECTION("move") {
        HeaderList<StringHeader> list(3), list2;
        list.add({"header1", "payload1"});
        list.add({"header2", "payload2"});
        list.add({"header3", "payload3"});
        auto handle = list.get_handle();
        list2 = std::move(list);
        CHECK_FALSE(!!list);
        CHECK(!!list2);
        CHECK(list2.size() == 3);
        CHECK(handle == list2.get_handle());
    }
}

TEST_CASE("access", "[headers]") {
    HeaderList<StringHeader> list(3);
    list.add({"header1", "payload1"});
    list.add({"header2", "payload2"});
    list.add({"header3", "payload3"});
    CHECK(list.at(0).get_value() == "payload1");
    CHECK(list.at(1).get_value() == "payload2");
    CHECK(list.at(2).get_value() == "payload3");
    CHECK_THROWS_AS(list.at(3), Exception);
    CHECK(list.front() == list.at(0));
    CHECK(list.back() == list.at(2));
}

TEST_CASE("iterate", "[headers]") {
    HeaderList<StringHeader> list(3);
    REQUIRE(list.begin() == list.end());
    list.add({"header1", "payload1"});
    REQUIRE(list.begin() != list.end());
    CHECK(++list.begin() == list.end());
    list.add({"header2", "payload2"});
    list.add({"header3", "payload3"});
    int i = 0;
    for (auto it = list.begin(); it != list.end(); ++it, ++i) {
        CHECK(it->get_name().length() == 7);
        if (i == 0) {
            CHECK(it->get_name() == "header1");
        }
        else if (i == 1) {
            CHECK(it->get_name() == "header2");
        }
        else if (i == 2) {
            CHECK(it->get_name() == "header3");
        }
    }
    //rewind end() iterator
    CHECK((--list.end())->get_name() == "header3");
}

#endif //RD_KAFKA_HEADERS_SUPPORT_VERSION


