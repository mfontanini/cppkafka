#ifndef CPPKAFKA_BUFFER_H
#define CPPKAFKA_BUFFER_H

#include <cstddef>

namespace cppkafka {

class Buffer {
public:
    using DataType = unsigned char;

    Buffer();
    Buffer(const DataType* data, size_t size);

    Buffer(const Buffer&) = delete;
    Buffer(Buffer&&) = default;
    Buffer& operator=(const Buffer&) = delete;
    Buffer& operator=(Buffer&&) = default;

    const DataType* get_data() const;
    size_t get_size() const;
private:
    const unsigned char* data_;
    size_t size_;
};

} // cppkafka

#endif // CPPKAFKA_BUFFER_H
