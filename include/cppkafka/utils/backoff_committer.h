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

#ifndef CPPKAFKA_BACKOFF_COMMITTER_H
#define CPPKAFKA_BACKOFF_COMMITTER_H

#include <chrono>
#include <functional>
#include <thread>
#include "../consumer.h"

namespace cppkafka {

/**
 * \brief Allows performing synchronous commits that will backoff until successful
 *
 * This class serves as a simple wrapper around Consumer::commit, allowing to commit
 * messages and topic/partition/offset tuples handling errors and performing a backoff
 * whenever the commit fails.
 *
 * Both linear and exponential backoff policies are supported, the former one being
 * the default.
 *
 * Example code on how to use this:
 *
 * \code
 * // Create a consumer
 * Consumer consumer(...);
 *
 * // Create a committer using this consumer
 * BackoffCommitter committer(consumer);
 *
 * // Set an error callback. This is optional and allows having some feedback 
 * // when commits fail. If the callback returns false, then this message won't
 * // be committer again.
 * committer.set_error_callback([](Error error) {
 *     cout << "Error committing: " << error << endl;
 *     return true; 
 * });
 *
 * // Now commit. If there's an error, this will retry forever
 * committer.commit(some_message);
 * \endcode
 */
class BackoffCommitter {
public:
    using TimeUnit = std::chrono::milliseconds;
    static constexpr TimeUnit DEFAULT_INITIAL_BACKOFF{100};
    static constexpr TimeUnit DEFAULT_BACKOFF_STEP{50};
    static constexpr TimeUnit DEFAULT_MAXIMUM_BACKOFF{1000};

    /**
     * \brief The error callback.
     * 
     * Whenever an error occurs comitting an offset, this callback will be executed using
     * the generated error. While the function returns true, then this is offset will be
     * committed again until it either succeeds or the function returns false.
     */
    using ErrorCallback = std::function<bool(Error)>;

    /**
     * The backoff policy to use
     */
    enum class BackoffPolicy {
        LINEAR,
        EXPONENTIAL
    };

    /**
     * \brief Constructs an instance using default values
     *
     * By default, the linear backoff policy is used
     *
     * \param consumer The consumer to use for committing offsets
     */
    BackoffCommitter(Consumer& consumer);

    /**
     * \brief Sets the backoff policy
     *
     * \param policy The backoff policy to be used
     */
    void set_backoff_policy(BackoffPolicy policy);

    /**
     * \brief Sets the initial backoff
     *
     * The first time a commit fails, this will be the delay between the request is sent
     * and we re-try doing so 
     *
     * \param value The value to be used
     */
    void set_initial_backoff(TimeUnit value);

    /**
     * \brief Sets the backoff step
     *
     * When using the linear backoff policy, this will be the delay between sending a request
     * that fails and re-trying it
     *
     * \param value The value to be used
     */
    void set_backoff_step(TimeUnit value);

    /**
     * \brief Sets the maximum backoff
     *
     * The backoff used will never be larger than this number
     *
     * \param value The value to be used
     */
    void set_maximum_backoff(TimeUnit value);

    /**
     * \brief Sets the error callback
     *
     * \sa ErrorCallback
     * \param callback The callback to be set
     */
    void set_error_callback(ErrorCallback callback);

    /**
     * \brief Commits the given message synchronously
     *
     * This will call Consumer::commit until either the message is successfully
     * committed or the error callback returns false (if any is set).
     *
     * \param msg The message to be committed
     */
    void commit(const Message& msg);

    /**
     * \brief Commits the offsets on the given topic/partitions synchronously
     *
     * This will call Consumer::commit until either the offsets are successfully
     * committed or the error callback returns false (if any is set).
     *
     * \param topic_partitions The topic/partition list to be committed
     */
    void commit(const TopicPartitionList& topic_partitions);
private:
    TimeUnit increase_backoff(TimeUnit backoff);

    template <typename T>
    void do_commit(const T& object) {
        TimeUnit backoff = initial_backoff_;
        while (true) {
            auto start = std::chrono::steady_clock::now();
            try {
                consumer_.commit(object);
                // If the commit succeeds, we're done
                return;
            }
            catch (const HandleException& ex) {
                // If there's a callback and it returns false for this message, abort
                if (callback_ && !callback_(ex.get_error())) {
                    return;
                }
            }

            auto end = std::chrono::steady_clock::now();
            auto time_elapsed = end - start;
            // If we still have time left, then sleep
            if (time_elapsed < backoff) {
                std::this_thread::sleep_for(backoff - time_elapsed);
            }
            // Increase out backoff depending on the policy being used
            backoff = increase_backoff(backoff);
        }
    }

    Consumer& consumer_;
    TimeUnit initial_backoff_;
    TimeUnit backoff_step_;
    TimeUnit maximum_backoff_;
    ErrorCallback callback_;
    BackoffPolicy policy_;
};

} // cppkafka

#endif // CPPKAFKA_BACKOFF_COMMITTER_H
