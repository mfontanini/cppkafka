#ifndef CPPKAFKA_CLONABLE_PTR_H
#define CPPKAFKA_CLONABLE_PTR_H

#include <memory>

namespace cppkafka {

template <typename T, typename Deleter, typename Cloner>
class ClonablePtr {
public:
    ClonablePtr(T* ptr, const Deleter& deleter, const Cloner& cloner)
    : handle_(ptr, deleter), cloner_(cloner) {

    }

    ClonablePtr(const ClonablePtr& rhs)
    : handle_(rhs.cloner_(rhs.handle_.get()), rhs.handle_.get_deleter()), cloner_(rhs.cloner_) {

    }

    ClonablePtr& operator=(const ClonablePtr& rhs) {
        handle_.reset(cloner_(rhs.handle_.get()));
        return *this;
    }

    ClonablePtr(ClonablePtr&&) = default;
    ClonablePtr& operator=(ClonablePtr&&) = default;
    ~ClonablePtr() = default;

    T* get() const {
        return handle_.get();
    }
private:
    std::unique_ptr<T, Deleter> handle_;
    Cloner cloner_;
};

} // cppkafka

#endif // CPPKAFKA_CLONABLE_PTR_H
