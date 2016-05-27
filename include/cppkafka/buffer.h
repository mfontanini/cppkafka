#ifndef CPPKAFKA_BUFFER_H
#define CPPKAFKA_BUFFER_H

#include <cstddef>
#include <algorithm>

namespace cppkafka {

class Buffer {
public:
    using DataType = unsigned char;

    Buffer();
    template <typename T>
    Buffer(const T* data, size_t size) 
    : data_(reinterpret_cast<const DataType*>(data)), size_(size) {
        static_assert(sizeof(T) == 1, "Buffer must point to elements of 1 byte");
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
