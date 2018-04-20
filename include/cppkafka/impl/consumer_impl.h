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

namespace cppkafka {

template <typename Traits>
void ConsumerHandle<Traits>::rebalance_proxy(rd_kafka_t*,
                                       rd_kafka_resp_err_t error,
                                       rd_kafka_topic_partition_list_t *partitions,
                                       void *opaque) {
    TopicPartitionList list = convert(partitions);
    static_cast<ConsumerHandle*>(opaque)->handle_rebalance(error, list);
}

template <typename Traits>
ConsumerHandle<Traits>::ConsumerHandle(typename Traits::config_type config)
: base_type(move(config)) {
    char error_buffer[512];
    rd_kafka_conf_t* config_handle = this->get_configuration_handle();
    // Set ourselves as the opaque pointer
    rd_kafka_conf_set_opaque(config_handle, this);
    rd_kafka_conf_set_rebalance_cb(config_handle, &ConsumerHandle<Traits>::rebalance_proxy);
    rd_kafka_t* ptr = rd_kafka_new(RD_KAFKA_CONSUMER, 
                                   rd_kafka_conf_dup(config_handle),
                                   error_buffer, sizeof(error_buffer));
    if (!ptr) {
        throw Exception("Failed to create consumer handle: " + string(error_buffer));
    }
    rd_kafka_poll_set_consumer(ptr);
    this->set_handle(ptr);
}

template <typename Traits>
ConsumerHandle<Traits>::~ConsumerHandle() {
    try {
        // make sure to destroy the function closures. in case they hold kafka
        // objects, they will need to be destroyed before we destroy the handle
        assignment_callback_ = nullptr;
        revocation_callback_ = nullptr;
        rebalance_error_callback_ = nullptr;
        close();
    }
    catch (const Exception&) {
        // If close throws just silently ignore until there's some
        // logging facility (if any)
    }
}

template <typename Traits>
void ConsumerHandle<Traits>::set_assignment_callback(AssignmentCallback callback) {
    assignment_callback_ = move(callback);
}

template <typename Traits>
void ConsumerHandle<Traits>::set_revocation_callback(RevocationCallback callback) {
    revocation_callback_ = move(callback);
}

template <typename Traits>
void ConsumerHandle<Traits>::set_rebalance_error_callback(RebalanceErrorCallback callback) {
    rebalance_error_callback_ = move(callback);
}

template <typename Traits>
void ConsumerHandle<Traits>::subscribe(const vector<string>& topics) {
    TopicPartitionList topic_partitions(topics.begin(), topics.end());
    TopicPartitionsListPtr topic_list_handle = convert(topic_partitions);
    rd_kafka_resp_err_t error = rd_kafka_subscribe(this->get_handle(), topic_list_handle.get());
    this->check_error(error);
}

template <typename Traits>
void ConsumerHandle<Traits>::unsubscribe() {
    rd_kafka_resp_err_t error = rd_kafka_unsubscribe(this->get_handle());
    this->check_error(error);
}

template <typename Traits>
void ConsumerHandle<Traits>::assign(const TopicPartitionList& topic_partitions) {
    TopicPartitionsListPtr topic_list_handle = convert(topic_partitions);
    // If the list is empty, then we need to use a null pointer
    auto handle = topic_partitions.empty() ? nullptr : topic_list_handle.get();
    rd_kafka_resp_err_t error = rd_kafka_assign(this->get_handle(), handle);
    this->check_error(error);
}

template <typename Traits>
void ConsumerHandle<Traits>::unassign() {
    rd_kafka_resp_err_t error = rd_kafka_assign(this->get_handle(), nullptr);
    this->check_error(error);
}

template <typename Traits>
void ConsumerHandle<Traits>::commit(const Message& msg) {
    commit(msg, false);
}

template <typename Traits>
void ConsumerHandle<Traits>::async_commit(const Message& msg) {
    commit(msg, true);
}

template <typename Traits>
void ConsumerHandle<Traits>::commit(const TopicPartitionList& topic_partitions) {
    commit(topic_partitions, false);
}

template <typename Traits>
void ConsumerHandle<Traits>::async_commit(const TopicPartitionList& topic_partitions) {
    commit(topic_partitions, true);
}

template <typename Traits>
typename HandleBase<Traits>::OffsetTuple
ConsumerHandle<Traits>::get_offsets(const TopicPartition& topic_partition) const {
    int64_t low;
    int64_t high;
    const string& topic = topic_partition.get_topic();
    const int partition = topic_partition.get_partition(); 
    rd_kafka_resp_err_t result = rd_kafka_get_watermark_offsets(this->get_handle(), topic.data(),
                                                                partition, &low, &high);
    this->check_error(result);
    return make_tuple(low, high);
}

template <typename Traits>
TopicPartitionList
ConsumerHandle<Traits>::get_offsets_committed(const TopicPartitionList& topic_partitions) const {
    TopicPartitionsListPtr topic_list_handle = convert(topic_partitions);
    rd_kafka_resp_err_t error = rd_kafka_committed(this->get_handle(), topic_list_handle.get(),
                                                   static_cast<int>(this->get_timeout().count()));
    this->check_error(error);
    return convert(topic_list_handle);
}

template <typename Traits>
TopicPartitionList
ConsumerHandle<Traits>::get_offsets_position(const TopicPartitionList& topic_partitions) const {
    TopicPartitionsListPtr topic_list_handle = convert(topic_partitions);
    rd_kafka_resp_err_t error = rd_kafka_position(this->get_handle(), topic_list_handle.get());
    this->check_error(error);
    return convert(topic_list_handle);
}

template <typename Traits>
vector<string> ConsumerHandle<Traits>::get_subscription() const {
    rd_kafka_resp_err_t error;
    rd_kafka_topic_partition_list_t* list = nullptr;
    error = rd_kafka_subscription(this->get_handle(), &list);
    this->check_error(error);

    auto handle = make_handle(list);
    vector<string> output;
    for (const auto& topic_partition : convert(handle)) {
        output.push_back(topic_partition.get_topic());
    }
    return output;
}

template <typename Traits>
TopicPartitionList ConsumerHandle<Traits>::get_assignment() const {
    rd_kafka_resp_err_t error;
    rd_kafka_topic_partition_list_t* list = nullptr;
    error = rd_kafka_assignment(this->get_handle(), &list);
    this->check_error(error);
    return convert(make_handle(list));
}

template <typename Traits>
string ConsumerHandle<Traits>::get_member_id() const {
    return rd_kafka_memberid(this->get_handle());
}

template <typename Traits>
const typename ConsumerHandle<Traits>::AssignmentCallback&
ConsumerHandle<Traits>::get_assignment_callback() const {
    return assignment_callback_;
}

template <typename Traits>
const typename ConsumerHandle<Traits>::RevocationCallback&
ConsumerHandle<Traits>::get_revocation_callback() const {
    return revocation_callback_;
}

template <typename Traits>
const typename ConsumerHandle<Traits>::RebalanceErrorCallback&
ConsumerHandle<Traits>::get_rebalance_error_callback() const {
    return rebalance_error_callback_;
}

template <typename Traits>
Message ConsumerHandle<Traits>::poll() {
    return poll(this->get_timeout());
}

template <typename Traits>
Message ConsumerHandle<Traits>::poll(milliseconds timeout) {
    rd_kafka_message_t* message = rd_kafka_consumer_poll(this->get_handle(),
                                                         static_cast<int>(timeout.count()));
    return message ? Message(message) : Message();
}

template <typename Traits>
vector<Message> ConsumerHandle<Traits>::poll_batch(size_t max_batch_size) {
    return poll_batch(max_batch_size, this->get_timeout());
}

template <typename Traits>
vector<Message> ConsumerHandle<Traits>::poll_batch(size_t max_batch_size, milliseconds timeout) {
    vector<rd_kafka_message_t*> raw_messages(max_batch_size);
    rd_kafka_queue_t* queue = rd_kafka_queue_get_consumer(this->get_handle());
    ssize_t result = rd_kafka_consume_batch_queue(queue, timeout.count(), raw_messages.data(),
                                                  raw_messages.size());
    if (result == -1) {
        this->check_error(rd_kafka_last_error());
        // on the off-chance that check_error() does not throw an error
        result = 0;
    }
    vector<Message> output;
    raw_messages.resize(result);
    output.reserve(result);
    for (const auto ptr : raw_messages) {
        output.emplace_back(ptr);
    }
    return output;
}

template <typename Traits>
void ConsumerHandle<Traits>::close() {
    rd_kafka_resp_err_t error = rd_kafka_consumer_close(this->get_handle());
    this->check_error(error);
}

template <typename Traits>
void ConsumerHandle<Traits>::commit(const Message& msg, bool async) {
    rd_kafka_resp_err_t error;
    error = rd_kafka_commit_message(this->get_handle(), msg.get_handle(),
                                    async ? 1 : 0);
    this->check_error(error);
}

template <typename Traits>
void ConsumerHandle<Traits>::commit(const TopicPartitionList& topic_partitions, bool async) {
    TopicPartitionsListPtr topic_list_handle = convert(topic_partitions);
    rd_kafka_resp_err_t error;
    error = rd_kafka_commit(this->get_handle(), topic_list_handle.get(), async ? 1 : 0);
    this->check_error(error);
}

template <typename Traits>
void ConsumerHandle<Traits>::handle_rebalance(rd_kafka_resp_err_t error,
                                TopicPartitionList& topic_partitions) {
    if (error == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
        if (assignment_callback_) {
            assignment_callback_(topic_partitions);
        }
        assign(topic_partitions);
    }
    else if (error == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS) {
        if (revocation_callback_) {
            revocation_callback_(topic_partitions);
        }
        unassign();
    }
    else {
        if (rebalance_error_callback_) {
            rebalance_error_callback_(error);
        }
        unassign();
    }
}

} // cppkafka
