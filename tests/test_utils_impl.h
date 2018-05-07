#include <mutex>
#include <chrono>
#include <condition_variable>
#include "test_utils.h"
#include "cppkafka/utils/consumer_dispatcher.h"

using std::vector;
using std::move;
using std::thread;
using std::mutex;
using std::lock_guard;
using std::unique_lock;
using std::condition_variable;
using std::chrono::system_clock;
using std::chrono::milliseconds;
using std::chrono::seconds;

using cppkafka::Consumer;
using cppkafka::BasicConsumerDispatcher;

using cppkafka::Message;
using cppkafka::MessageList;
using cppkafka::TopicPartition;

//==================================================================================
//                           BasicConsumerRunner
//==================================================================================
template <typename ConsumerType>
BasicConsumerRunner<ConsumerType>::BasicConsumerRunner(ConsumerType& consumer,
                                                       size_t expected,
                                                       size_t partitions)
: consumer_(consumer) {
    bool booted = false;
    mutex mtx;
    condition_variable cond;
    thread_ = thread([&, expected, partitions]() {
        consumer_.set_timeout(milliseconds(500));
        size_t number_eofs = 0;
        auto start = system_clock::now();
        BasicConsumerDispatcher<ConsumerType> dispatcher(consumer_);
        dispatcher.run(
            // Message callback
            [&](Message msg) {
                if (number_eofs == partitions) {
                    messages_.push_back(move(msg));
                }
            },
            // EOF callback
            [&](typename BasicConsumerDispatcher<ConsumerType>::EndOfFile, const TopicPartition& topic_partition) {
                if (number_eofs != partitions) {
                    number_eofs++;
                    if (number_eofs == partitions) {
                        lock_guard<mutex> _(mtx);
                        booted = true;
                        cond.notify_one();
                    }
                }
            },
            // Every time there's any event callback
            [&](typename BasicConsumerDispatcher<ConsumerType>::Event) {
                if (expected > 0 && messages_.size() == expected) {
                    dispatcher.stop();
                }
                if (expected == 0 && number_eofs >= partitions) {
                    dispatcher.stop();
                }
                if (system_clock::now() - start >= seconds(20)) {
                    dispatcher.stop();
                }
            }
        );
        // dispatcher has stopped
        if (number_eofs < partitions) {
            lock_guard<mutex> _(mtx);
            booted = true;
            cond.notify_one();
        }
    });

    unique_lock<mutex> lock(mtx);
    while (!booted) {
        cond.wait(lock);
    }
}

template <typename ConsumerType>
BasicConsumerRunner<ConsumerType>::~BasicConsumerRunner() {
    try_join();
}

template <typename ConsumerType>
const MessageList& BasicConsumerRunner<ConsumerType>::get_messages() const {
    return messages_;
}

template <typename ConsumerType>
void BasicConsumerRunner<ConsumerType>::try_join() {
    if (thread_.joinable()) {
        thread_.join();
    }
}

//==================================================================================
//                           PollStrategyAdapter
//==================================================================================
inline
PollStrategyAdapter::PollStrategyAdapter(Configuration config)
 : Consumer(config) {
}

inline
void PollStrategyAdapter::add_polling_strategy(std::unique_ptr<PollInterface> poll_strategy) {
    strategy_ = std::move(poll_strategy);
}

inline
void PollStrategyAdapter::delete_polling_strategy() {
    strategy_.reset();
}

inline
Message PollStrategyAdapter::poll() {
    if (strategy_) {
        return strategy_->poll();
    }
    return Consumer::poll();
}

inline
Message PollStrategyAdapter::poll(milliseconds timeout) {
    if (strategy_) {
        return strategy_->poll(timeout);
    }
    return Consumer::poll(timeout);
}

inline
MessageList PollStrategyAdapter::poll_batch(size_t max_batch_size) {
    if (strategy_) {
        return strategy_->poll_batch(max_batch_size);
    }
    return Consumer::poll_batch(max_batch_size);
}

inline
MessageList PollStrategyAdapter::poll_batch(size_t max_batch_size,
                                           milliseconds timeout) {
    if (strategy_) {
        return strategy_->poll_batch(max_batch_size, timeout);
    }
    return Consumer::poll_batch(max_batch_size, timeout);
}

inline
void PollStrategyAdapter::set_timeout(milliseconds timeout) {
    if (strategy_) {
        strategy_->set_timeout(timeout);
    }
    else {
        Consumer::set_timeout(timeout);
    }
}

inline
milliseconds PollStrategyAdapter::get_timeout() {
    if (strategy_) {
        return strategy_->get_timeout();
    }
    return Consumer::get_timeout();
}


