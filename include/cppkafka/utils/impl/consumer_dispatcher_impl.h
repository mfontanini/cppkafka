//
// Created by adamian on 4/7/18.
//

#ifndef CPPKAFKA_CONSUMER_DISPATCHER_IMPL_H
#define CPPKAFKA_CONSUMER_DISPATCHER_IMPL_H

namespace cppkafka {

template <typename ConsumerType>
BasicConsumerDispatcher<ConsumerType>::BasicConsumerDispatcher(ConsumerType& consumer)
: consumer_(consumer) {

}

template <typename ConsumerType>
void BasicConsumerDispatcher<ConsumerType>::stop() {
    running_ = false;
}

template <typename ConsumerType>
void BasicConsumerDispatcher<ConsumerType>::handle_error(Error error) {
    throw ConsumerException(error);
}

template <typename ConsumerType>
template <typename... Args>
void BasicConsumerDispatcher<ConsumerType>::run(const Args&... args) {
    using self = BasicConsumerDispatcher<ConsumerType>;
    
    // Make sure all callbacks match one of the signatures. Otherwise users could provide
    // bogus callbacks that would never be executed
    check_callbacks_match(args...);

    // This one is required
    const auto on_message = find_matching_functor<OnMessageArgs>(args...);

    // For the rest, append our own implementation at the end as a fallback
    const auto on_error = find_matching_functor<OnErrorArgs>(args..., &self::handle_error);
    const auto on_eof = find_matching_functor<OnEofArgs>(args..., &self::handle_eof);
    const auto on_timeout = find_matching_functor<OnTimeoutArgs>(args..., &self::handle_timeout);
    const auto on_event = find_matching_functor<OnEventArgs>(args..., &self::handle_event);

    running_ = true;
    while (running_) {
        Message msg = consumer_.poll();
        if (!msg) {
            on_timeout(Timeout{});
        }
        else if (msg.get_error()) {
            if (msg.is_eof()) {
                on_eof(EndOfFile{}, { msg.get_topic(), msg.get_partition(), msg.get_offset() });
            }
            else {
                on_error(msg.get_error());
            }
        }
        else {
            process_message(on_message, std::move(msg), args...);
        }
        on_event(Event{});
    }
}

template <typename ConsumerType>
template <typename Functor>
void BasicConsumerDispatcher<ConsumerType>::handle_throttle(Throttle, const Functor& callback, Message msg) {
    BackoffPerformer{}.perform([&]() {
        if (!running_) {
            return true;
        }
        msg = callback(std::move(msg));
        if (msg) {
            // Poll so we send heartbeats to the brokers
            consumer_.poll();
        }
        return !msg;
    });
}

template <typename ConsumerType>
template <typename Functor, typename... Args>
const Functor& BasicConsumerDispatcher<ConsumerType>::find_functor(const Args&... args) {
    return find_functor_helper<Functor>::find(args...);
}

template <typename ConsumerType>
template <typename Tuple, typename... Functors>
const typename BasicConsumerDispatcher<ConsumerType>::template find_type<Tuple, Functors...>::type&
BasicConsumerDispatcher<ConsumerType>::find_matching_functor(const Functors&... functors) {
    using type = typename find_type<Tuple, Functors...>::type;
    static_assert(!std::is_same<type_not_found, type>::value, "Valid functor not found");
    return find_functor<type>(functors...);
}

template <typename ConsumerType>
template <typename Functor>
void BasicConsumerDispatcher<ConsumerType>::check_callback_matches(const Functor& functor) {
    static_assert(
        !std::is_same<type_not_found,
                      typename find_type<OnMessageArgs, Functor>::type>::value ||
        !std::is_same<type_not_found,
                      typename find_type<OnEofArgs, Functor>::type>::value ||
        !std::is_same<type_not_found,
                      typename find_type<OnTimeoutArgs, Functor>::type>::value ||
        !std::is_same<type_not_found,
                      typename find_type<OnErrorArgs, Functor>::type>::value ||
        !std::is_same<type_not_found,
                      typename find_type<OnEventArgs, Functor>::type>::value,
        "Callback doesn't match any of the expected signatures"
    );
}

template <typename ConsumerType>
template <typename Functor, typename... Functors>
void BasicConsumerDispatcher<ConsumerType>::check_callbacks_match(const Functor& functor, const Functors&... functors) {
    check_callback_matches(functor);
    check_callbacks_match(functors...);
}

} //namespace

#endif //CPPKAFKA_CONSUMER_DISPATCHER_IMPL_H
