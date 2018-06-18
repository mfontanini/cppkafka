#include <string>
#include <vector>
#include <sstream>
#include <catch.hpp>
#include "cppkafka/buffer.h"

using std::string;
using std::vector;
using std::ostringstream;

using namespace cppkafka;

TEST_CASE("conversions", "[buffer]") {
    const string data = "Hello world!";
    const Buffer buffer(data);
    const Buffer empty_buffer;

    SECTION("construction") {
        CHECK_THROWS_AS(Buffer((const char*)nullptr, 5), Exception);
    }

    SECTION("bool conversion") {
        CHECK(!!buffer == true);
        CHECK(!!empty_buffer == false);
    }

    SECTION("string conversion") {
        CHECK(static_cast<string>(buffer) == data);
        CHECK(static_cast<string>(empty_buffer).empty());
    }

    SECTION("vector conversion") {
        const vector<char> buffer_as_vector = buffer;
        CHECK(string(buffer_as_vector.begin(), buffer_as_vector.end()) == data);
    }
}

TEST_CASE("construction", "[buffer]") {
    const string str_data = "Hello world!";
    const vector<uint8_t> data(str_data.begin(), str_data.end());
    const Buffer buffer(data);
    CHECK(str_data == buffer);
}


TEST_CASE("comparison", "[buffer]") {
    const string data = "Hello world!";
    const Buffer buffer1(data);
    const Buffer buffer2(data);
    const Buffer empty_buffer;

    SECTION("equality") {
        CHECK(buffer1 == buffer2);
        CHECK(buffer2 == buffer1);
    }

    SECTION("inequality") {
        CHECK(buffer1 != empty_buffer);
        CHECK(empty_buffer != buffer1);
    }
}

TEST_CASE("stream extraction", "[buffer]") {
    const string data = "Hello \x7fwor\x03ld!";
    const string pretty_string = "Hello \\x7fwor\\x03ld!";
    const Buffer buffer(data);

    ostringstream output;
    output << buffer;
    CHECK(output.str() == pretty_string );
}
