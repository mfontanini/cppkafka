#include <string>
#include <vector>
#include <sstream>
#include <gtest/gtest.h>
#include "cppkafka/buffer.h"

using std::string;
using std::vector;
using std::ostringstream;

using namespace cppkafka;

class BufferTest : public testing::Test {
public:
    
};

TEST_F(BufferTest, OperatorBool) {
    string data = "Hello world!";
    Buffer buffer1(data);
    Buffer buffer2;

    EXPECT_TRUE(buffer1);
    EXPECT_FALSE(buffer2);
}

TEST_F(BufferTest, StringConversion) {
    string data = "Hello world!";
    Buffer buffer(data);
    string buffer_as_string = buffer;
    EXPECT_EQ(data, buffer_as_string);
}

TEST_F(BufferTest, StringConversionOnEmptyBuffer) {
    Buffer buffer;
    EXPECT_EQ("", static_cast<string>(buffer));
}

TEST_F(BufferTest, VectorConversion) {
    string data = "Hello world!";
    Buffer buffer(data);
    vector<char> buffer_as_vector = buffer;
    EXPECT_EQ(data, string(buffer_as_vector.begin(), buffer_as_vector.end()));
}

TEST_F(BufferTest, Equality) {
    string data = "Hello world!";
    Buffer buffer1(data);
    Buffer buffer2(data);

    EXPECT_EQ(buffer1, buffer2);
}

TEST_F(BufferTest, InEquality) {
    string data1 = "Hello world!";
    string data2 = "Hello worldz";
    Buffer buffer1(data1);
    Buffer buffer2(data2);

    EXPECT_NE(buffer1, buffer2);
}

TEST_F(BufferTest, OutputOperator) {
    string data = "Hello \x7fwor\x03ld!";
    string pretty_string = "Hello \\x7fwor\\x03ld!";
    Buffer buffer(data);

    ostringstream output;
    output << buffer;
    EXPECT_EQ(pretty_string, output.str());
}
