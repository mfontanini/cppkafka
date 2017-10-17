#ifndef CPPKAFKA_MOCKING_API_H
#define CPPKAFKA_MOCKING_API_H

#include <librdkafka/rdkafka.h>
#include <cppkafka/mocking/handle_wrapper.h>
#include <cppkafka/mocking/configuration_mock.h>
#include <cppkafka/mocking/handle_mock.h>

struct rd_kafka_conf_s : cppkafka::mocking::HandleWrapper<cppkafka::mocking::ConfigurationMock> {
    using cppkafka::mocking::HandleWrapper<cppkafka::mocking::ConfigurationMock>::HandleWrapper;
};

struct rd_kafka_topic_conf_s : rd_kafka_conf_s {
    using rd_kafka_conf_s::rd_kafka_conf_s;
};

struct rd_kafka_s : cppkafka::mocking::HandleWrapper<cppkafka::mocking::HandleMock> {
    using cppkafka::mocking::HandleWrapper<cppkafka::mocking::HandleMock>::HandleWrapper;
};

#endif // CPPKAFKA_MOCKING_API_H
