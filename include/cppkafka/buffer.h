#ifndef CPPKAFKA_BUFFER_H
#define CPPKAFKA_BUFFER_H

namespace cppkafka {

class Buffer {
public:
    using DataType = unsigned char;

    Buffer();
    Buffer(const std::string& data);
    Buffer(const DataType* data, size_t size);

    Buffer(const Buffer&) = delete;
    Buffer(Buffer&&) = delete;
    Buffer& operator=(const Buffer&) = delete;
    Buffer& operator=(Buffer&&) = delete;

    const DataType* get_data() const;
    size_t get_size() const;
private:
    const unsigned char* data_;
    size_t size_;
};

} // cppkafka

#endif // CPPKAFKA_BUFFER_H
