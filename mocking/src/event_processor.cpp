#include <cppkafka/mocking/event_processor.h>

using std::lock_guard;
using std::unique_lock;
using std::mutex;
using std::move;

namespace cppkafka {
namespace mocking {

EventProcessor::EventProcessor()
: processing_thread_(&EventProcessor::process_events, this) {

}

EventProcessor::~EventProcessor() {
    {
        lock_guard<mutex> _(events_mutex_);
        running_ = false;
        events_condition_.notify_one();
    }
    processing_thread_.join();
}

void EventProcessor::add_event(EventPtr event) {
    lock_guard<mutex> _(events_mutex_);
    events_.push(move(event));
    events_condition_.notify_one();
}

void EventProcessor::process_events() {
    while (true) {
        unique_lock<mutex> lock(events_mutex_);
        while (running_ && events_.empty()) {
            events_condition_.wait(lock);
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
