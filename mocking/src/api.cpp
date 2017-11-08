#include <string>
#include <algorithm>
#include <stdexcept>
#include <cstring>
#include <cstdarg>
#include <cppkafka/mocking/api.h>
#include <cppkafka/mocking/producer_mock.h>
#include <cppkafka/mocking/consumer_mock.h>
#include <cppkafka/mocking/kafka_cluster_registry.h>

using std::string;
using std::copy;
using std::vector;
using std::strlen;
using std::move;
using std::make_shared;
using std::unique_ptr;
using std::tie;
using std::exception;

using std::chrono::milliseconds;

using namespace cppkafka::mocking;
using namespace cppkafka::mocking::detail;

// rd_kafka_conf_t

rd_kafka_conf_t* rd_kafka_conf_new() {
    return new rd_kafka_conf_t();
}

void rd_kafka_conf_destroy(rd_kafka_conf_t* conf) {
    delete conf;
}

rd_kafka_conf_t* rd_kafka_conf_dup(const rd_kafka_conf_t* conf) {
    return new rd_kafka_conf_t(conf->get_handle());
}

rd_kafka_conf_res_t rd_kafka_conf_set(rd_kafka_conf_t* conf,
                                      const char* name,
                                      const char* value,
                                      char*, size_t) {
    conf->get_handle().set(name, value);
    return RD_KAFKA_CONF_OK;
}

rd_kafka_conf_res_t rd_kafka_conf_get(const rd_kafka_conf_t* conf,
                                      const char* name_raw,
                                      char* dest, size_t* dest_size) {
    const string name = name_raw;
    if (!conf->get_handle().has_key(name)) {
        return RD_KAFKA_CONF_UNKNOWN;
    }
    if (dest == nullptr) {
        *dest_size = conf->get_handle().get(name).size();
    }
    else {
        const string value = conf->get_handle().get(name);
        if (value.size() > *dest_size + 1) {
            return RD_KAFKA_CONF_INVALID;
        }
        else {
            copy(value.begin(), value.end(), dest);
        }
    }
    return RD_KAFKA_CONF_OK;
}

void rd_kafka_conf_set_dr_msg_cb(rd_kafka_conf_t* conf,
                                 ConfigurationMock::DeliveryReportCallback* cb) {
    conf->get_handle().set_delivery_report_callback(cb);
}

void rd_kafka_conf_set_rebalance_cb(rd_kafka_conf_t* conf,
                                    ConfigurationMock::RebalanceCallback* cb) {
    conf->get_handle().set_rebalance_callback(cb);
}

void rd_kafka_conf_set_offset_commit_cb(rd_kafka_conf_t* conf,
                                        ConfigurationMock::OffsetCommitCallback* cb) {
    conf->get_handle().set_offset_commit_callback(cb);
}

void rd_kafka_conf_set_error_cb(rd_kafka_conf_t* conf,
                                ConfigurationMock::ErrorCallback* cb) {
    conf->get_handle().set_error_callback(cb);
}

void rd_kafka_conf_set_throttle_cb(rd_kafka_conf_t* conf,
                                   ConfigurationMock::ThrottleCallback* cb) {
    conf->get_handle().set_throttle_callback(cb);
}

void rd_kafka_conf_set_log_cb(rd_kafka_conf_t* conf,
                              ConfigurationMock::LogCallback* cb) {
    conf->get_handle().set_log_callback(cb);
}

void rd_kafka_conf_set_stats_cb(rd_kafka_conf_t* conf,
                                ConfigurationMock::StatsCallback* cb) {
    conf->get_handle().set_stats_callback(cb);
}

void rd_kafka_conf_set_socket_cb(rd_kafka_conf_t* conf,
                                 ConfigurationMock::SocketCallback* cb) {
    conf->get_handle().set_socket_callback(cb);
}

void rd_kafka_topic_conf_set_partitioner_cb(rd_kafka_topic_conf_t* conf,
                                            ConfigurationMock::PartitionerCallback* cb) {
    conf->get_handle().set_partitioner_callback(cb);
}

void rd_kafka_conf_set_default_topic_conf(rd_kafka_conf_t* conf,
                                          rd_kafka_topic_conf_t* tconf) {
    conf->get_handle().set_default_topic_configuration(tconf->get_handle());
    rd_kafka_topic_conf_destroy(tconf);
}

void rd_kafka_conf_set_opaque(rd_kafka_conf_t* conf, void* opaque) {
    conf->get_handle().set_opaque(opaque);
}

void rd_kafka_topic_conf_set_opaque(rd_kafka_topic_conf_t* conf, void* opaque) {
    conf->get_handle().set_opaque(opaque);
}

const char** rd_kafka_conf_dump(rd_kafka_conf_t* conf, size_t* cntp) {
    const auto options = conf->get_handle().get_options();
    *cntp = options.size() * 2;
    // Allocate enough for all (key, value) pairs
    char** output = new char*[*cntp];
    size_t i = 0;
    const auto set_value = [&](const string& value) {
        output[i] = new char[value.size() + 1];
        copy(value.begin(), value.end(), output[i]);
        ++i;
    };
    for (const auto& option : options) {
        set_value(option.first);
        set_value(option.second);
    }
    return const_cast<const char**>(output);
}

void rd_kafka_conf_dump_free(const char** arr, size_t cnt) {
    for (size_t i = 0; i < cnt; ++i) {
        delete[] arr[i];
    }
    delete[] arr;
}

// rd_kafka_topic_conf_t
rd_kafka_topic_conf_t* rd_kafka_topic_conf_new() {
    return new rd_kafka_topic_conf_t();
}

void rd_kafka_topic_conf_destroy(rd_kafka_topic_conf_t* conf) {
    delete conf;
}

rd_kafka_topic_conf_t* rd_kafka_topic_conf_dup(const rd_kafka_topic_conf_t* conf) {
    return new rd_kafka_topic_conf_t(conf->get_handle());
}

rd_kafka_conf_res_t rd_kafka_topic_conf_set(rd_kafka_topic_conf_t* conf,
                                            const char* name,
                                            const char* value,
                                            char* errstr, size_t errstr_size) {
    return rd_kafka_conf_set(conf, name, value, errstr, errstr_size);
}

rd_kafka_conf_res_t rd_kafka_topic_conf_get(const rd_kafka_topic_conf_t *conf,
                                            const char *name, char *dest, size_t *dest_size) {
    return rd_kafka_conf_get(conf, name, dest, dest_size);
}

const char** rd_kafka_topic_conf_dump(rd_kafka_topic_conf_t* conf, size_t* cntp) {
    return rd_kafka_conf_dump(conf, cntp);
}

// rd_kafka_topic_*

void rd_kafka_topic_partition_destroy (rd_kafka_topic_partition_t* toppar) {
    delete toppar->topic;
    delete toppar;
}

rd_kafka_topic_partition_list_t* rd_kafka_topic_partition_list_new(int size) {
    rd_kafka_topic_partition_list_t* output = new rd_kafka_topic_partition_list_t{};
    output->size = size;
    output->elems = new rd_kafka_topic_partition_t[size];
    for (int i = 0; i < size; ++i) {
        output->elems[i] = {};
    }
    return output;
}

void rd_kafka_topic_partition_list_destroy(rd_kafka_topic_partition_list_t* toppar_list) {
    for (int i = 0; i < toppar_list->cnt; ++i) {
        delete[] toppar_list->elems[i].topic;
    }
    delete[] toppar_list->elems;
    delete toppar_list;
}

rd_kafka_topic_partition_t*
rd_kafka_topic_partition_list_add(rd_kafka_topic_partition_list_t* toppar_list,
                                  const char* topic, int32_t partition) {
    if (toppar_list->cnt >= toppar_list->size) {
        return nullptr;
    }
    rd_kafka_topic_partition_t* output = &toppar_list->elems[toppar_list->cnt++];
    const size_t length = strlen(topic);
    output->topic = new char[length + 1];
    copy(topic, topic + length, output->topic);
    output->topic[length] = 0;
    output->partition = partition;
    output->offset = RD_KAFKA_OFFSET_INVALID;
    return output;
}

// rd_kafka_topic_t

rd_kafka_topic_t* rd_kafka_topic_new(rd_kafka_t* rk, const char* topic,
                                     rd_kafka_topic_conf_t* conf) {
    return reinterpret_cast<rd_kafka_topic_t*>(new TopicHandle(topic, nullptr));
}

const char* rd_kafka_topic_name(const rd_kafka_topic_t* rkt) {
    return reinterpret_cast<const TopicHandle*>(rkt)->get_topic().c_str();
}

void rd_kafka_topic_destroy(rd_kafka_topic_t* rkt) {
    delete reinterpret_cast<TopicHandle*>(rkt);
}

int rd_kafka_topic_partition_available(const rd_kafka_topic_t* rkt, int32_t partition) {
    return 1;
}

// rd_kafka_t

rd_kafka_t* rd_kafka_new(rd_kafka_type_t type, rd_kafka_conf_t* conf_ptr,
                         char *errstr, size_t errstr_size) {
    static const string BROKERS_OPTION = "metadata.broker.list";
    auto& conf = conf_ptr->get_handle();
    HandleMock::ClusterPtr cluster;
    if (conf.has_key(BROKERS_OPTION)) {
        cluster = KafkaClusterRegistry::instance().get_cluster(conf.get(BROKERS_OPTION));
    }
    if (type == RD_KAFKA_PRODUCER) {
        unique_ptr<rd_kafka_conf_t> _(conf_ptr);
        return new rd_kafka_t(new ProducerMock(move(conf), make_shared<EventProcessor>(),
                                               move(cluster)));
    }
    else if (type == RD_KAFKA_CONSUMER) {
        if (!conf.has_key("group.id")) {
            const string error = "Local: Unknown topic";
            if (error.size() < errstr_size) {
                copy(error.begin(), error.end(), errstr);
                errstr[error.size()] = 0;
            }
            return nullptr;
        }
        unique_ptr<rd_kafka_conf_t> _(conf_ptr);
        return new rd_kafka_t(new ConsumerMock(move(conf), make_shared<EventProcessor>(),
                                               move(cluster)));   
    }
    return nullptr;
}

void rd_kafka_destroy(rd_kafka_t* rk) {
    delete rk;
}

int rd_kafka_brokers_add(rd_kafka_t* rk, const char* brokerlist) {
    auto cluster = KafkaClusterRegistry::instance().get_cluster(brokerlist);
    if (cluster) {
        rk->get_handle().set_cluster(move(cluster));
    }
    return 1;
}

const char* rd_kafka_name(const rd_kafka_t* rk) {
    return "cppkafka mock handle";
}

rd_kafka_message_t* rd_kafka_consumer_poll(rd_kafka_t* rk, int timeout_ms) {
    auto& consumer = rk->get_handle<ConsumerMock>();
    auto message_ptr = consumer.poll(milliseconds(timeout_ms));
    if (!message_ptr) {
        return nullptr;
    }
    else {
        return &message_ptr.release()->get_message();
    }
}

void rd_kafka_message_destroy(rd_kafka_message_t* rkmessage) {
    delete static_cast<MessageHandlePrivateData*>(rkmessage->_private)->get_owner();
}

rd_kafka_resp_err_t rd_kafka_pause_partitions(rd_kafka_t* rk,
                                              rd_kafka_topic_partition_list_t* partitions) {
    const vector<TopicPartitionMock> topic_partitions = from_rdkafka_handle(*partitions);
    auto& consumer = rk->get_handle<ConsumerMock>();
    consumer.pause_partitions(topic_partitions);
    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

rd_kafka_resp_err_t rd_kafka_resume_partitions(rd_kafka_t* rk,
                                               rd_kafka_topic_partition_list_t* partitions) {
    const vector<TopicPartitionMock> topic_partitions = from_rdkafka_handle(*partitions);
    auto& consumer = rk->get_handle<ConsumerMock>();
    consumer.resume_partitions(topic_partitions);
    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

rd_kafka_resp_err_t rd_kafka_subscribe(rd_kafka_t* rk,
                                       const rd_kafka_topic_partition_list_t* partitions) {
    const vector<TopicPartitionMock> topic_partitions = from_rdkafka_handle(*partitions);
    vector<string> topics;
    for (const TopicPartitionMock& topic_partition : topic_partitions) {
        topics.emplace_back(topic_partition.get_topic());
    }
    auto& consumer = rk->get_handle<ConsumerMock>();
    consumer.subscribe(topics);
    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

rd_kafka_resp_err_t rd_kafka_unsubscribe(rd_kafka_t* rk) {
    auto& consumer = rk->get_handle<ConsumerMock>();
    consumer.unsubscribe();
    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

rd_kafka_resp_err_t rd_kafka_subscription(rd_kafka_t* rk,
                                          rd_kafka_topic_partition_list_t** topics) {
    // TODO: implement
    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

rd_kafka_resp_err_t rd_kafka_assign(rd_kafka_t* rk,
                                    const rd_kafka_topic_partition_list_t* partitions) {
    auto& consumer = rk->get_handle<ConsumerMock>();
    if (partitions) {
        const vector<TopicPartitionMock> topic_partitions = from_rdkafka_handle(*partitions);
        consumer.assign(topic_partitions);
    }
    else {
        consumer.unassign();
    }
    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

rd_kafka_resp_err_t rd_kafka_assignment(rd_kafka_t* rk,
                                        rd_kafka_topic_partition_list_t** partitions) {
    auto& consumer = rk->get_handle<ConsumerMock>();
    const vector<TopicPartitionMock> assignment = consumer.get_assignment();
    *partitions = to_rdkafka_handle(assignment).release();
    return RD_KAFKA_RESP_ERR_NO_ERROR;   
}

rd_kafka_resp_err_t rd_kafka_flush(rd_kafka_t* rk, int timeout_ms) {
    if (rk->get_handle<ProducerMock>().flush(milliseconds(timeout_ms))) {
        return RD_KAFKA_RESP_ERR_NO_ERROR;
    }
    else {
        return RD_KAFKA_RESP_ERR__TIMED_OUT;
    }
}

int rd_kafka_poll(rd_kafka_t* rk, int timeout_ms) {
    return rk->get_handle<ProducerMock>().poll(milliseconds(timeout_ms));
}

rd_kafka_queue_t* rd_kafka_queue_get_consumer(rd_kafka_t* rk) {
    // TODO: implement
    return nullptr;
}

ssize_t rd_kafka_consume_batch_queue(rd_kafka_queue_t* rkqu, int timeout_ms,
                                     rd_kafka_message_t** rkmessages, size_t rkmessages_size) {
    // TODO: implement
    return 0;
}

rd_kafka_resp_err_t rd_kafka_producev(rd_kafka_t* rk, ...) {
    va_list args;
    int vtype;
    unique_ptr<TopicHandle> topic;
    unsigned partition = RD_KAFKA_PARTITION_UA;
    void* key_ptr = nullptr;
    size_t key_size = 0;
    void* payload_ptr = nullptr;
    size_t payload_size = 0;
    void* opaque = nullptr;
    MessageHandle::PointerOwnership ownership = MessageHandle::PointerOwnership::Unowned;
    int64_t timestamp = 0;

    va_start(args, rk);
    while ((vtype = va_arg(args, int)) != RD_KAFKA_VTYPE_END) {
        switch (vtype) {
        case RD_KAFKA_VTYPE_TOPIC:
            topic.reset(new TopicHandle(va_arg(args, const char *), nullptr));
            break;
        case RD_KAFKA_VTYPE_PARTITION:
            partition = va_arg(args, int32_t);
            break;
        case RD_KAFKA_VTYPE_VALUE:
            payload_ptr = va_arg(args, void *);
            payload_size = va_arg(args, size_t);
            break;
        case RD_KAFKA_VTYPE_KEY:
            key_ptr = va_arg(args, void *);
            key_size = va_arg(args, size_t);
            break;
        case RD_KAFKA_VTYPE_OPAQUE:
            opaque = va_arg(args, void *);
            break;
        case RD_KAFKA_VTYPE_MSGFLAGS:
            if (va_arg(args, int) == static_cast<int>(MessageHandle::PointerOwnership::Owned)) {
                ownership = MessageHandle::PointerOwnership::Owned;
            }
            break;
        case RD_KAFKA_VTYPE_TIMESTAMP:
            timestamp = va_arg(args, int64_t);
            break;
        default:
            return RD_KAFKA_RESP_ERR__INVALID_ARG;
        }
    }
    va_end(args);

    MessageHandlePrivateData private_data(RD_KAFKA_TIMESTAMP_CREATE_TIME, timestamp);
    private_data.set_opaque(opaque);
    rk->get_handle<ProducerMock>().produce(MessageHandle(
        move(topic),
        partition,
        -1, // offset
        key_ptr, key_size,
        payload_ptr, payload_size,
        RD_KAFKA_RESP_ERR_NO_ERROR,
        private_data,
        ownership
    ));
    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

int rd_kafka_outq_len(rd_kafka_t* rk) {
    return rk->get_handle<ProducerMock>().get_event_count();
}

void* rd_kafka_opaque(const rd_kafka_t* rk) {
    return rk->get_handle().get_opaque();
}

void rd_kafka_set_log_level(rd_kafka_t* /*rk*/, int /*level*/) {

}

rd_kafka_resp_err_t rd_kafka_query_watermark_offsets(rd_kafka_t* rk, const char* topic,
                                                     int32_t partition, int64_t* low,
                                                     int64_t* high, int /*timeout_ms*/) {
    return rd_kafka_get_watermark_offsets(rk, topic, partition, low, high);
}

rd_kafka_resp_err_t rd_kafka_get_watermark_offsets(rd_kafka_t* rk, const char *topic,
                                                   int32_t partition, int64_t *low,
                                                   int64_t *high) {
    const auto& cluster = rk->get_handle().get_cluster();
    if (!cluster.topic_exists(topic)) {
        return RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC;
    }
    try {
        const auto& topic_object = cluster.get_topic(topic);
        if (static_cast<unsigned>(partition) >= topic_object.get_partition_count()) {
            return RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION;
        }
        const auto& partition_object = topic_object.get_partition(partition);
        int64_t lowest;
        int64_t largest;
        tie(lowest, largest) = partition_object.get_offset_bounds();
        *low = lowest;
        *high = largest;
        return RD_KAFKA_RESP_ERR_NO_ERROR;
    }
    catch (const exception&) {
        return RD_KAFKA_RESP_ERR_UNKNOWN;
    }
}

rd_kafka_resp_err_t rd_kafka_offsets_for_times(rd_kafka_t* rk,
                                               rd_kafka_topic_partition_list_t* offsets,
                                               int timeout_ms) {
    // TODO: implement this one
    return RD_KAFKA_RESP_ERR_UNKNOWN;
}

rd_kafka_resp_err_t rd_kafka_metadata(rd_kafka_t* rk, int all_topics,
                                      rd_kafka_topic_t* only_rkt,
                                      const struct rd_kafka_metadata** metadatap,
                                      int timeout_ms) {
    // TODO: implement this one
    return RD_KAFKA_RESP_ERR_UNKNOWN;
}

void rd_kafka_metadata_destroy(const struct rd_kafka_metadata* /*metadata*/) {
    // TODO: implement this one
}

rd_kafka_resp_err_t rd_kafka_list_groups(rd_kafka_t* rk, const char* group,
                                         const struct rd_kafka_group_list** grplistp,
                                         int timeout_ms) {
    // TODO: implement this one
    return RD_KAFKA_RESP_ERR_UNKNOWN;
}

void rd_kafka_group_list_destroy(const struct rd_kafka_group_list* /*grplist*/) {
    // TODO: implement this one
}

rd_kafka_resp_err_t rd_kafka_poll_set_consumer(rd_kafka_t*) {
    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

rd_kafka_resp_err_t rd_kafka_consumer_close(rd_kafka_t* rk) {
    rk->get_handle<ConsumerMock>().close();
    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

rd_kafka_resp_err_t rd_kafka_committed(rd_kafka_t* rk, rd_kafka_topic_partition_list_t *partitions,
                                       int timeout_ms) {
    // TODO: implement
    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

rd_kafka_resp_err_t rd_kafka_commit(rd_kafka_t* rk, const rd_kafka_topic_partition_list_t* offsets,
                                    int async) {
    rk->get_handle<ConsumerMock>().commit(from_rdkafka_handle(*offsets));
    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

rd_kafka_resp_err_t rd_kafka_commit_message(rd_kafka_t* rk, const rd_kafka_message_t* message,
                                            int async) {
    rk->get_handle<ConsumerMock>().commit(*message);
    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

rd_kafka_resp_err_t rd_kafka_position(rd_kafka_t* rk,
                                      rd_kafka_topic_partition_list_t* partitions) {
    // TODO: implement
    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

char* rd_kafka_memberid(const rd_kafka_t* rk) {
    // TODO: make this better
    char* output = (char*)malloc(strlen("cppkafka_mock") + 1);
    strcpy(output, "cppkafka_mock");
    return output;
}

// misc

const char* rd_kafka_err2str(rd_kafka_resp_err_t err) {
    return "cppkafka mock: error";
}

rd_kafka_resp_err_t rd_kafka_errno2err(int errnox) {
    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

int32_t rd_kafka_msg_partitioner_consistent_random(const rd_kafka_topic_t* rkt, const void *key,
                                                   size_t keylen, int32_t partition_cnt,
                                                   void *opaque, void *msg_opaque) {
    unsigned hash = 0;
    const char* key_ptr = reinterpret_cast<const char*>(key);
    for (size_t i = 0; i < keylen; ++i) {
        hash += key_ptr[i];
    }
    return hash % partition_cnt;
}

rd_kafka_resp_err_t rd_kafka_last_error (void) {
    // TODO: fix this
    return RD_KAFKA_RESP_ERR_UNKNOWN;
}
