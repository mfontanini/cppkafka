#ifndef CPPKAFKA_TEST_UTILS_H
#define CPPKAFKA_TEST_UTILS_H

#include <string>
#include <thread>
#include <vector>
#include "cppkafka/consumer.h"
#include "cppkafka/utils/roundrobin_poll_strategy.h"
#include "cppkafka/utils/consumer_dispatcher.h"

extern const std::vector<std::string> KAFKA_TOPICS;
extern const int KAFKA_NUM_PARTITIONS;

using namespace cppkafka;

//==================================================================================
//                           BasicConsumerRunner
//==================================================================================
template <typename ConsumerType>
class BasicConsumerRunner {
public:
    BasicConsumerRunner(ConsumerType& consumer,
                        size_t expected,
                        size_t partitions);
    BasicConsumerRunner(const BasicConsumerRunner&) = delete;
    BasicConsumerRunner& operator=(const BasicConsumerRunner&) = delete;
    ~BasicConsumerRunner();

    const std::vector<cppkafka::Message>& get_messages() const;

    void try_join();
private:
    ConsumerType& consumer_;
    std::thread thread_;
    std::vector<cppkafka::Message> messages_;
};

//==================================================================================
//                           PollStrategyAdapter
//==================================================================================
/**
 * \brief Specific implementation which can be used with other
 *        util classes such as BasicConsumerDispatcher.
 */
class PollStrategyAdapter : public Consumer {
public:
    PollStrategyAdapter(Configuration config);
    void add_polling_strategy(std::unique_ptr<PollInterface> poll_strategy);
    void delete_polling_strategy();
    Message poll();
    Message poll(std::chrono::milliseconds timeout);
    std::vector<Message> poll_batch(size_t max_batch_size);
    std::vector<Message> poll_batch(size_t max_batch_size,
                                    std::chrono::milliseconds timeout);
    void set_timeout(std::chrono::milliseconds timeout);
    std::chrono::milliseconds get_timeout();
private:
    std::unique_ptr<PollInterface> strategy_;
};

// Misc

std::string make_consumer_group_id();

using PollConsumerRunner = BasicConsumerRunner<PollStrategyAdapter>;
using ConsumerRunner = BasicConsumerRunner<Consumer>;


#include "test_utils_impl.h"

#endif // CPPKAFKA_TEST_UTILS_H
