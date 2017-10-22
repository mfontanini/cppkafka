#include <string>
#include <algorithm>
#include <cstring>
#include <cppkafka/mocking/api.h>
#include <cppkafka/mocking/producer_mock.h>
#include <cppkafka/mocking/kafka_cluster_registry.h>

using std::string;
using std::copy;
using std::strlen;
using std::make_shared;

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

void rd_kafka_topic_conf_set_partitioner_cb(rd_kafka_conf_t* conf,
                                            ConfigurationMock::PartitionerCallback* cb) {
    conf->get_handle().set_partitioner_callback(cb);
}

void rd_kafka_conf_set_default_topic_conf(rd_kafka_conf_t* conf,
                                          rd_kafka_topic_conf_t* tconf) {
    conf->get_handle().set_default_topic_configuration(tconf->get_handle());
}

void rd_kafka_conf_set_opaque(rd_kafka_conf_t* conf, void* opaque) {
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
        delete toppar_list->elems[i].topic;
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
    output->partition = partition;
    output->offset = RD_KAFKA_OFFSET_INVALID;
    return output;
}

// rd_kafka_t

rd_kafka_t* rd_kafka_new(rd_kafka_type_t type, rd_kafka_conf_t *conf_ptr,
                         char *errstr, size_t errstr_size) {
    static const string BROKERS_OPTION = "metadata.broker.list";
    const auto& conf = conf_ptr->get_handle();
    HandleMock::ClusterPtr cluster;
    if (conf.has_key(BROKERS_OPTION)) {
        cluster = KafkaClusterRegistry::instance().get_cluster(conf.get(BROKERS_OPTION));
    }
    if (type == RD_KAFKA_PRODUCER) {
        return new rd_kafka_t(new ProducerMock(conf, make_shared<EventProcessor>(),
                                               move(cluster)));
    }
    return nullptr;
}
