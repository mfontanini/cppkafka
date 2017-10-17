#ifndef CPPKAFKA_MOCKING_HANDLE_WRAPPER_H
#define CPPKAFKA_MOCKING_HANDLE_WRAPPER_H

#include <utility>
#include <memory>

namespace cppkafka {
namespace mocking {

template <typename T>
class HandleWrapper {
public:
    HandleWrapper()
    : handle_(new T()) {

    }

    template <typename... Args>
    HandleWrapper(const Args&... args)
    : handle_(new T(args...)) {

    }

    template <typename U>
    explicit HandleWrapper(U* ptr)
    : handle_(ptr) {

    }



    T& get_handle() {
        return *handle_;
    }

    const T& get_handle() const {
        return *handle_;
    }
private:
    std::unique_ptr<T> handle_;
};

} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_HANDLE_WRAPPER_H
