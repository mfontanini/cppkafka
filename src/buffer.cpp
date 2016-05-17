#include "buffer.h"

using std::string;

namespace cppkafka {

Buffer::Buffer() 
: data_(nullptr), size_(0) {

}

Buffer::Buffer(const string& data)
: data_(data.data()), size_(data.size()) {

}

Buffer::Buffer(const DataType* data, size_t size) 
: data_(data), size_(size) {

}

const Buffer::DataType* Buffer::get_data() const {
    return data_;
}

size_t Buffer::get_size() const {
    return size_;
}

} // cppkafka
