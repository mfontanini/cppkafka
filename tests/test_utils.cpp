#include <mutex>
#include <chrono>
#include <condition_variable>
#include "test_utils.h"

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
using cppkafka::Message;

ConsumerRunner::ConsumerRunner(Consumer& consumer, size_t expected, size_t partitions) 
: consumer_(consumer) {
    bool booted = false;
    mutex mtx;
    condition_variable cond;
    thread_ = thread([&, expected, partitions]() {
        consumer_.set_timeout(milliseconds(500));
        size_t number_eofs = 0;
        auto start = system_clock::now();
        while (system_clock::now() - start < seconds(20)) {
            if (expected > 0 && messages_.size() == expected) {
                break;
            }
            if (expected == 0 && number_eofs >= partitions) {
                break;
            }
            Message msg = consumer_.poll();
            if (msg && number_eofs != partitions &&
                msg.get_error() == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                number_eofs++;
                if (number_eofs == partitions) {
                    lock_guard<mutex> _(mtx);
                    booted = true;
                    cond.notify_one();
                }
            }
            else if (msg && !msg.get_error() && number_eofs == partitions) {
                messages_.push_back(move(msg));
            }
        }
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

ConsumerRunner::~ConsumerRunner() {
    try_join();
}

const vector<Message>& ConsumerRunner::get_messages() const {
    return messages_;
}

void ConsumerRunner::try_join() {
    if (thread_.joinable()) {
        thread_.join();
    }
}

