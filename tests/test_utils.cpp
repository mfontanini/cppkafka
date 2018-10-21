#include <cstdint>
#include <iomanip>
#include <limits>
#include <sstream>
#include <random>
#include "test_utils.h"

using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::seconds;
using std::chrono::system_clock;
using std::hex;
using std::move;
using std::numeric_limits;
using std::ostringstream;
using std::random_device;
using std::string;
using std::uniform_int_distribution;
using std::unique_ptr;
using std::vector;

//==================================================================================
//                           PollStrategyAdapter
//==================================================================================

PollStrategyAdapter::PollStrategyAdapter(Configuration config)
 : Consumer(config) {
}

void PollStrategyAdapter::add_polling_strategy(unique_ptr<PollInterface> poll_strategy) {
    strategy_ = move(poll_strategy);
}

void PollStrategyAdapter::delete_polling_strategy() {
    strategy_.reset();
}

Message PollStrategyAdapter::poll() {
    if (strategy_) {
        return strategy_->poll();
    }
    return Consumer::poll();
}

Message PollStrategyAdapter::poll(milliseconds timeout) {
    if (strategy_) {
        return strategy_->poll(timeout);
    }
    return Consumer::poll(timeout);
}

vector<Message> PollStrategyAdapter::poll_batch(size_t max_batch_size) {
    if (strategy_) {
        return strategy_->poll_batch(max_batch_size);
    }
    return Consumer::poll_batch(max_batch_size);
}

vector<Message> PollStrategyAdapter::poll_batch(size_t max_batch_size, milliseconds timeout) {
    if (strategy_) {
        return strategy_->poll_batch(max_batch_size, timeout);
    }
    return Consumer::poll_batch(max_batch_size, timeout);
}

void PollStrategyAdapter::set_timeout(milliseconds timeout) {
    if (strategy_) {
        strategy_->set_timeout(timeout);
    }
    else {
        Consumer::set_timeout(timeout);
    }
}

milliseconds PollStrategyAdapter::get_timeout() {
    if (strategy_) {
        return strategy_->get_timeout();
    }
    return Consumer::get_timeout();
}

// Misc

string make_consumer_group_id() {
    ostringstream output;
    output << hex;

    random_device rd;
    uniform_int_distribution<uint64_t> distribution(0, numeric_limits<uint64_t>::max());
    const auto now = duration_cast<seconds>(system_clock::now().time_since_epoch());
    const auto random_number = distribution(rd);
    output << now.count() << random_number;
    return output.str();
}
