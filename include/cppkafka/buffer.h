#ifndef CPPKAFKA_BUFFER_H
#define CPPKAFKA_BUFFER_H

#include <cstddef>
#include <algorithm>

namespace cppkafka {

class Buffer {
public:
    using DataType = unsigned char;

    Buffer();
    Buffer(const DataType* data, size_t size);
    template <typename ForwardIterator>
    Buffer(const ForwardIterator& start, const ForwardIterator& end) :
        data_((const DataType*)&*start), size_(std::distance(start, end)) {

    }

    Buffer(const Buffer&) = delete;
    Buffer(Buffer&&) = default;
    Buffer& operator=(const Buffer&) = delete;
    Buffer& operator=(Buffer&&) = default;

    const DataType* get_data() const;
    size_t get_size() const;

    std::string as_string() const;
private:
    const DataType* data_;
    size_t size_;
};

} // cppkafka

#endif // CPPKAFKA_BUFFER_H
