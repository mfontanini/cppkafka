#ifndef CPPKAFKA_MOCKING_API_H
#define CPPKAFKA_MOCKING_API_H

#include <librdkafka/rdkafka.h>
#include <cppkafka/mocking/handle_wrapper.h>
#include <cppkafka/mocking/configuration_mock.h>
#include <cppkafka/macros.h>

struct rd_kafka_conf_s : cppkafka::mocking::HandleWrapper<cppkafka::mocking::ConfigurationMock> {
    using cppkafka::mocking::HandleWrapper<cppkafka::mocking::ConfigurationMock>::HandleWrapper;
};

struct rd_kafka_topic_conf_s : rd_kafka_conf_s {
    using rd_kafka_conf_s::rd_kafka_conf_s;
};

// rd_kafka_conf_t
CPPKAFKA_API rd_kafka_conf_t* rd_kafka_conf_new();
CPPKAFKA_API void rd_kafka_conf_destroy(rd_kafka_conf_t* conf);
CPPKAFKA_API rd_kafka_conf_t* rd_kafka_conf_dup(const rd_kafka_conf_t* conf);
CPPKAFKA_API rd_kafka_conf_res_t rd_kafka_conf_set(rd_kafka_conf_t* conf,
                                                   const char* name, const char* value,
                                                   char* errstr, size_t errstr_size);
CPPKAFKA_API rd_kafka_conf_res_t rd_kafka_conf_get(const rd_kafka_conf_t* conf,
                                                   const char* name, char* dest,
                                                   size_t* dest_size);
CPPKAFKA_API
void rd_kafka_conf_set_dr_msg_cb(rd_kafka_conf_t* conf,
                                 cppkafka::mocking::ConfigurationMock::DeliveryReportCallback* cb);
CPPKAFKA_API
void rd_kafka_conf_set_rebalance_cb(rd_kafka_conf_t* conf,
                                    cppkafka::mocking::ConfigurationMock::RebalanceCallback* cb);
CPPKAFKA_API
void rd_kafka_conf_set_offset_commit_cb(rd_kafka_conf_t* conf,
                                    cppkafka::mocking::ConfigurationMock::OffsetCommitCallback* cb);
CPPKAFKA_API
void rd_kafka_conf_set_error_cb(rd_kafka_conf_t* conf,
                                cppkafka::mocking::ConfigurationMock::ErrorCallback* cb);
CPPKAFKA_API
void rd_kafka_conf_set_throttle_cb(rd_kafka_conf_t* conf,
                                   cppkafka::mocking::ConfigurationMock::ThrottleCallback* cb);
CPPKAFKA_API
void rd_kafka_conf_set_log_cb(rd_kafka_conf_t* conf,
                              cppkafka::mocking::ConfigurationMock::LogCallback* cb);
CPPKAFKA_API
void rd_kafka_conf_set_stats_cb(rd_kafka_conf_t* conf,
                                cppkafka::mocking::ConfigurationMock::StatsCallback* cb);
CPPKAFKA_API
void rd_kafka_conf_set_socket_cb(rd_kafka_conf_t* conf,
                                 cppkafka::mocking::ConfigurationMock::SocketCallback* cb);
CPPKAFKA_API
void rd_kafka_topic_conf_set_partitioner_cb(rd_kafka_conf_t* conf,
                                    cppkafka::mocking::ConfigurationMock::PartitionerCallback* cb);
CPPKAFKA_API
void rd_kafka_conf_set_default_topic_conf(rd_kafka_conf_t* conf,
                                          rd_kafka_topic_conf_t* tconf);
CPPKAFKA_API void rd_kafka_conf_set_opaque(rd_kafka_conf_t* conf, void* opaque);
CPPKAFKA_API const char** rd_kafka_conf_dump(rd_kafka_conf_t* conf, size_t* cntp);
CPPKAFKA_API void rd_kafka_conf_dump_free(const char** arr, size_t cnt);

// rd_kafka_topic_conf_t
CPPKAFKA_API rd_kafka_topic_conf_t* rd_kafka_topic_conf_new();
CPPKAFKA_API void rd_kafka_topic_conf_destroy(rd_kafka_topic_conf_t* conf);
CPPKAFKA_API rd_kafka_topic_conf_t* rd_kafka_topic_conf_dup(const rd_kafka_topic_conf_t* conf);
CPPKAFKA_API rd_kafka_conf_res_t rd_kafka_topic_conf_set(rd_kafka_topic_conf_t* conf,
                                                         const char* name, const char* value,
                                                         char* errstr, size_t errstr_size);
CPPKAFKA_API rd_kafka_conf_res_t rd_kafka_topic_conf_get(const rd_kafka_topic_conf_t *conf,
                                                         const char *name, char *dest,
                                                         size_t *dest_size);
CPPKAFKA_API
const char** rd_kafka_topic_conf_dump(rd_kafka_topic_conf_t* conf, size_t* cntp);

#endif // CPPKAFKA_MOCKING_API_H
