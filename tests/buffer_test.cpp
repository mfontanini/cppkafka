#include <string>
#include <vector>
#include <array>
#include <sstream>
#include <catch.hpp>
#include "cppkafka/buffer.h"

using std::string;
using std::vector;
using std::array;
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
    // From string
    const string str_data = "Hello world!";
    // From vector
    const vector<uint8_t> vector_data(str_data.begin(), str_data.end());
    // From array
    const array<char,12> array_data{'H','e','l','l','o',' ','w','o','r','l','d','!'};
    // From raw array
    const char raw_array[12]{'H','e','l','l','o',' ','w','o','r','l','d','!'};
    
    // Build buffers
    const Buffer buffer(vector_data); //vector
    const Buffer buffer2(vector_data.begin(), vector_data.end()); //iterators
    const Buffer buffer3(str_data.data(), str_data.data() + str_data.size()); //char iterators
    const Buffer buffer4(array_data); //arrays
    const Buffer buffer5(raw_array); //raw arrays
    const Buffer buffer6(str_data); //string
    const Buffer buffer7(str_data.data(), str_data.size()); //type + size
    
    // Test
    CHECK(str_data == buffer);
    CHECK(buffer == buffer2);
    CHECK(buffer == buffer3);
    CHECK(buffer == buffer4);
    CHECK(buffer == buffer5);
    CHECK(buffer == buffer6);
    CHECK(buffer == buffer7);
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
