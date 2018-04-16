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


    SECTION("bool conversion") {
        REQUIRE(!!buffer == true);
        REQUIRE(!!empty_buffer == false);
    }

    SECTION("string conversion") {
        REQUIRE(static_cast<string>(buffer) == data);
        REQUIRE(static_cast<string>(empty_buffer).empty());
    }

    SECTION("vector conversion") {
        const vector<char> buffer_as_vector = buffer;
        REQUIRE(string(buffer_as_vector.begin(), buffer_as_vector.end()) == data);
    }
}

TEST_CASE("construction", "[buffer]") {
    const string str_data = "Hello world!";
    const vector<uint8_t> data(str_data.begin(), str_data.end());
    const Buffer buffer(data);
    REQUIRE(str_data == buffer);
}


TEST_CASE("comparison", "[buffer]") {
    const string data = "Hello world!";
    const Buffer buffer1(data);
    const Buffer buffer2(data);
    const Buffer empty_buffer;

    SECTION("equality") {
        REQUIRE(buffer1 == buffer2);
        REQUIRE(buffer2 == buffer1);
    }

    SECTION("inequality") {
        REQUIRE(buffer1 != empty_buffer);
        REQUIRE(empty_buffer != buffer1);
    }
}

TEST_CASE("stream extraction", "[buffer]") {
    const string data = "Hello \x7fwor\x03ld!";
    const string pretty_string = "Hello \\x7fwor\\x03ld!";
    const Buffer buffer(data);

    ostringstream output;
    output << buffer;
    REQUIRE(output.str() == pretty_string );
}
