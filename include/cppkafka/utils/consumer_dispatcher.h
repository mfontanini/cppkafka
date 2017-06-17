/*
 * Copyright (c) 2017, Matias Fontanini
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * * Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above
 *   copyright notice, this list of conditions and the following disclaimer
 *   in the documentation and/or other materials provided with the
 *   distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#ifndef CPPKAFKA_CONSUMER_DISPATCHER_H
#define CPPKAFKA_CONSUMER_DISPATCHER_H

#include <tuple>
#include "../consumer.h"
#include "backoff_performer.h"

namespace cppkafka {

/**
 * \brief Helper to perform pattern matching when consuming messages
 *
 * As the way to consume messages requires you to:
 * 
 * * Poll for a message
 * * Check if it's not null
 * * Check if it's an error (optionally handling EOF as a non error)
 * * Process the message
 *
 * This class introduces a pattern matching based approach to consuming messages
 * so the usual loop is simplified away and you can process messages without
 * having to check for all those cases.
 *
 * When calling ConsumerDispatcher::run, a list of callbacks has to be provided.
 * These will handle each case (message, timeout, error, eof), allowing you to
 * only provide what you need. The only callback that is required is the message one.
 * For the rest, the following actions will be performed as defaults:
 *
 * * Timeout: ignore
 * * EOF: ignore
 * * Error (not an EOF error): throw a ConsumerException exception
 *
 * The signature for each callback should be as following (or compatible)
 *
 * * Message callback, either:
 *  - void(Message)
 *  - Message(Message). In this case if the message is returned, it will be buffered
 *  while calling the throttle callback until the message is actually processed.
 * * Timeout: void(ConsumerDispatcher::Timeout)
 * * Error: void(Error)
 * * EOF: void(ConsumerDispatcher::EndOfFile, TopicPartition)
 */
class ConsumerDispatcher {
public:
    /**
     * Tag to indicate a timeout occurred
     */
    struct Timeout {};

    /**
     * Tag to indicate end of file was reached on a partition being consumed
     */
    struct EndOfFile {};

    /*
     * Tag to indicate end of file was reached on a partition being consumed
     */
    struct Throttle {};

    /**
     * Constructs a consumer dispatcher over the given consumer
     *
     * \param consumer The consumer to be used
     */
    ConsumerDispatcher(Consumer& consumer);

    /**
     * \brief Consumes messages dispatching events to the appropriate callack
     *
     * This will loop until ConsumerDispatcher::stop is called
     *
     * \param args The list of callbacks to be executed
     */
    template <typename... Args>
    void run(const Args&... args);

    /**
     * \brief Stops consumption
     *
     * Note that as this is synchronous, if there's any poll operations currently in
     * progress, then this will stop after the current call returns
     */
    void stop();
private:
    // Define the types we need for each type of callback
    using OnMessageArgs = std::tuple<Message>;
    using OnErrorArgs = std::tuple<Error>;
    using OnEofArgs = std::tuple<EndOfFile, TopicPartition>;
    using OnTimeoutArgs = std::tuple<Timeout>;

    static void handle_error(Error error);
    static void handle_eof(EndOfFile, const TopicPartition& /*topic_partition*/) { }
    static void handle_timeout(Timeout) { }
    template <typename Functor>
    void handle_throttle(Throttle, const Functor& callback, Message msg) {
        BackoffPerformer{}.perform([&]() {
            if (!running_) {
                return true;
            }
            msg = callback(std::move(msg));
            return !msg;
        });
    }

    // Traits and template helpers

    // Finds whether type T accepts arguments of types Args...
    template <typename T, typename... Args>
    struct takes_arguments {
        using yes = double;
        using no = bool;

        template <typename Functor>
        static yes test(decltype(std::declval<Functor&>()(std::declval<Args>()...))*);
        template <typename Functor>
        static no test(...);

        static constexpr bool value = sizeof(test<T>(nullptr)) == sizeof(yes);
    };

    // Specialization for tuple
    template <typename T, typename... Args>
    struct takes_arguments<T, std::tuple<Args...>> : takes_arguments<T, Args...> {

    };

    template <typename T>
    struct identity {
        using type = T;
    };

    // Placeholder to indicate a type wasn't found
    struct type_not_found {

    };

    // find_type: given a tuple of types and a list of functors, finds the functor
    // type that accepts the given tuple types as parameters
    template <typename Tuple, typename Functor, typename... Functors>
    struct find_type_helper {
        using type = typename std::conditional<takes_arguments<Functor, Tuple>::value,
                                               identity<Functor>,
                                               find_type_helper<Tuple, Functors...>
                                              >::type::type;
    };

    template <typename Tuple>
    struct find_type_helper<Tuple, type_not_found> {
        using type = type_not_found;
    };

    template <typename Tuple, typename... Functors>
    struct find_type {
        using type = typename find_type_helper<Tuple, Functors..., type_not_found>::type;
    };

    // find_functor: given a Functor and a template parameter pack of functors, finds
    // the one that matches the given type
    template <typename Functor>
    struct find_functor_helper {
        template <typename... Functors>
        static const Functor& find(const Functor& arg, Functors&&...) {
            return arg;
        }

        template <typename Head, typename... Functors>
        static typename std::enable_if<!std::is_same<Head, Functor>::value, const Functor&>::type
        find(const Head&, const Functors&... functors) {
            return find(functors...);
        }
    };

    template <typename Functor, typename... Args>
    const Functor& find_functor(const Args&... args) {
        return find_functor_helper<Functor>::find(args...);
    }

    // Finds the first functor that accepts the parameters in a tuple and returns it. If no
    // such functor is found, a static asertion will occur
    template <typename Tuple, typename... Functors>
    const typename find_type<Tuple, Functors...>::type&
    find_matching_functor(const Functors&... functors) {
        using type = typename find_type<Tuple, Functors...>::type;
        static_assert(!std::is_same<type_not_found, type>::value, "Valid functor not found");
        return find_functor<type>(functors...);
    }

    // Check that a given functor matches at least one of the expected signatures
    template <typename Functor>
    void check_callback_matches(const Functor& functor) {
        static_assert(
            !std::is_same<type_not_found,
                          typename find_type<OnMessageArgs, Functor>::type>::value ||
            !std::is_same<type_not_found,
                          typename find_type<OnEofArgs, Functor>::type>::value ||
            !std::is_same<type_not_found,
                          typename find_type<OnTimeoutArgs, Functor>::type>::value ||
            !std::is_same<type_not_found,
                          typename find_type<OnErrorArgs, Functor>::type>::value,
            "Callback doesn't match any of the expected signatures"
        );
    }

    // Base case for recursion
    void check_callbacks_match() {

    }

    // Check that all given functors match at least one of the expected signatures
    template <typename Functor, typename... Functors>
    void check_callbacks_match(const Functor& functor, const Functors&... functors) {
        check_callback_matches(functor);
        check_callbacks_match(functors...);
    }

    template <typename Functor, typename... Functors>
    auto process_message(const Functor& callback, Message msg, const Functors&...) 
    -> typename std::enable_if<std::is_same<void, decltype(callback(std::move(msg)))>::value,
                               void>::type {
        callback(std::move(msg));
    }

    template <typename Functor, typename... Functors>
    auto process_message(const Functor& callback, Message msg, const Functors&... functors)
    -> typename std::enable_if<std::is_same<Message, decltype(callback(std::move(msg)))>::value,
                               void>::type { 
        const auto throttle_ptr = &ConsumerDispatcher::handle_throttle<Functor>;
        const auto default_throttler = std::bind(throttle_ptr, this, std::placeholders::_1,
                                                 std::placeholders::_2, std::placeholders::_3);

        using OnThrottleArgs = std::tuple<Throttle, const Functor&, Message>;  
        const auto on_throttle = find_matching_functor<OnThrottleArgs>(functors...,
                                                                       default_throttler);
        
        msg = callback(std::move(msg));
        if (msg) {
            on_throttle(Throttle{}, callback, std::move(msg));
        }
    }

    Consumer& consumer_;
    bool running_;
};

template <typename... Args>
void ConsumerDispatcher::run(const Args&... args) {
    using self = ConsumerDispatcher;
    
    // Make sure all callbacks match one of the signatures. Otherwise users could provide
    // bogus callbacks that would never be executed
    check_callbacks_match(args...);

    // This one is required
    const auto on_message = find_matching_functor<OnMessageArgs>(args...);

    // For the rest, append our own implementation at the end as a fallback
    const auto on_error = find_matching_functor<OnErrorArgs>(args..., &self::handle_error);
    const auto on_eof = find_matching_functor<OnEofArgs>(args..., &self::handle_eof);
    const auto on_timeout = find_matching_functor<OnTimeoutArgs>(args..., &self::handle_timeout);

    running_ = true;
    while (running_) {
        Message msg = consumer_.poll();
        if (!msg) {
            on_timeout(Timeout{});
            continue;
        }
        if (msg.get_error()) {
            if (msg.is_eof()) {
                on_eof(EndOfFile{}, { msg.get_topic(), msg.get_partition(), msg.get_offset() });
            }
            else {
                on_error(msg.get_error());
            }
            continue;
        }
        process_message(on_message, std::move(msg), args...);
    }
}

} // cppkafka

#endif // CPPKAFKA_CONSUMER_DISPATCHER_H
