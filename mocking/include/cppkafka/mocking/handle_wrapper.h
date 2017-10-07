#ifndef CPPKAFKA_MOCKING_HANDLE_WRAPPER_H
#define CPPKAFKA_MOCKING_HANDLE_WRAPPER_H

#include <utility>

namespace cppkafka {
namespace mocking {

template <typename T>
class HandleWrapper {
public:
    template <typename... Args>
    HandleWrapper(Args&&... args)
    : handle_(std::forward<Args>(args)...) {

    }

    T& get_handle() {
        return handle_;
    }

    const T& get_handle() const {
        return handle_;
    }
private:
    T handle_;
};

} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_HANDLE_WRAPPER_H
