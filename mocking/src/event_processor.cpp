#include <cppkafka/mocking/event_processor.h>

using std::lock_guard;
using std::unique_lock;
using std::mutex;
using std::move;

using std::chrono::milliseconds;

namespace cppkafka {
namespace mocking {

EventProcessor::EventProcessor()
: processing_thread_(&EventProcessor::process_events, this) {

}

EventProcessor::~EventProcessor() {
    {
        lock_guard<mutex> _(events_mutex_);
        running_ = false;
        new_events_condition_.notify_all();
        no_events_condition_.notify_all();
    }
    processing_thread_.join();
}

void EventProcessor::add_event(EventPtr event) {
    lock_guard<mutex> _(events_mutex_);
    events_.push(move(event));
    new_events_condition_.notify_one();
}

size_t EventProcessor::get_event_count() const {
    lock_guard<mutex> _(events_mutex_);
    return events_.size();
}

bool EventProcessor::wait_until_empty(milliseconds timeout) {
    unique_lock<mutex> lock(events_mutex_);
    if (running_ && !events_.empty()) {
        no_events_condition_.wait_for(lock, timeout);
    }
    return events_.empty();
}

void EventProcessor::process_events() {
    while (true) {
        unique_lock<mutex> lock(events_mutex_);
        while (running_ && events_.empty()) {
            new_events_condition_.wait(lock);
        }

        if (!running_) {
            break;
        }
        EventPtr event = move(events_.front());
        events_.pop();

        lock.unlock();
        event->execute();
    }
}

} // mocking
} // cppkafka
