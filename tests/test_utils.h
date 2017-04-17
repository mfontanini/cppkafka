#ifndef CPPKAFKA_TEST_UTILS_H
#define CPPKAFKA_TEST_UTILS_H

#include <thread>
#include <vector>
#include "cppkafka/consumer.h"

class ConsumerRunner {
public:
    ConsumerRunner(cppkafka::Consumer& consumer, size_t expected, size_t partitions);
    ConsumerRunner(const ConsumerRunner&) = delete;
    ConsumerRunner& operator=(const ConsumerRunner&) = delete;
    ~ConsumerRunner();

    const std::vector<cppkafka::Message>& get_messages() const;

    void try_join();
private:
    cppkafka::Consumer& consumer_;
    std::thread thread_;
    std::vector<cppkafka::Message> messages_;
};

#endif // CPPKAFKA_TEST_UTILS_H
