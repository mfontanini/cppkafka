#ifndef CPPKAFKA_MOCKING_CONFIGURATION_MOCK_H
#define CPPKAFKA_MOCKING_CONFIGURATION_MOCK_H

#include <librdkafka/rdkafka.h>
#include <unordered_map>
#include <string>
#include <cppkafka/clonable_ptr.h>

namespace cppkafka {
namespace mocking {

class ConfigurationMock {
public:
    using DeliveryReportCallback = void(rd_kafka_t*, const rd_kafka_message_t*, void *);
    using RebalanceCallback = void(rd_kafka_t*, rd_kafka_resp_err_t,
                                   rd_kafka_topic_partition_list_t*, void*);
    using OffsetCommitCallback = void(rd_kafka_t*, rd_kafka_resp_err_t,
                                      rd_kafka_topic_partition_list_t*, void*);
    using ErrorCallback = void(rd_kafka_t*, int, const char*, void*);
    using ThrottleCallback = void(rd_kafka_t*, const char*, int32_t, int, void*);
    using LogCallback = void(const rd_kafka_t*, int, const char*, const char*);
    using StatsCallback = int(rd_kafka_t*, char*, size_t, void*);
    using SocketCallback = int(int, int, int, void*);
    using PartitionerCallback = int32_t(const rd_kafka_topic_t*, const void*,
                                        size_t, int32_t, void*, void*);

    ConfigurationMock();

    void set(std::string key, std::string value);
    size_t has_key(const std::string& key) const;
    std::string get(const std::string& key) const;

    void set_delivery_report_callback(DeliveryReportCallback* callback);
    void set_rebalance_callback(RebalanceCallback* callback);
    void set_offset_commit_callback(OffsetCommitCallback* callback);
    void set_error_callback(ErrorCallback* callback);
    void set_throttle_callback(ThrottleCallback* callback);
    void set_log_callback(LogCallback* callback);
    void set_stats_callback(StatsCallback* callback);
    void set_socket_callback(SocketCallback* callback);
    void set_partitioner_callback(PartitionerCallback* callback);
    void set_default_topic_configuration(const ConfigurationMock& conf);
    void set_opaque(void* ptr);

    DeliveryReportCallback* get_delivery_report_callback() const;
    RebalanceCallback* get_rebalance_callback() const;
    OffsetCommitCallback* get_offset_commit_callback() const;
    ErrorCallback* get_error_callback() const;
    ThrottleCallback* get_throttle_callback() const;
    LogCallback* get_log_callback() const;
    StatsCallback* get_stats_callback() const;
    SocketCallback* get_socket_callback() const;
    PartitionerCallback* get_partitioner_callback() const;
    const ConfigurationMock* get_default_topic_configuration() const;
    void* get_opaque() const;
    std::unordered_map<std::string, std::string> get_options() const;
private:
    enum class Callback {
        DeliveryReport,
        Rebalance,
        OffsetCommit,
        Error,
        Throttle,
        Log,
        Stats,
        Socket,
        Partitioner
    };

    struct Hasher {
        size_t operator()(Callback c) const {
            return static_cast<size_t>(c);
        }
    };

    struct Cloner {
        ConfigurationMock* operator()(const ConfigurationMock* ptr) const {
            return new ConfigurationMock(*ptr);
        }
    };

    using TopicConfigirationPtr = ClonablePtr<ConfigurationMock,
                                              std::default_delete<ConfigurationMock>,
                                              Cloner>;

    template <typename Functor>
    void set_callback(Callback type, Functor* ptr);
    template <typename Functor>
    Functor* get_callback(Callback type) const;

    std::unordered_map<std::string, std::string> options_;
    std::unordered_map<Callback, void*, Hasher> callbacks_;
    TopicConfigirationPtr default_topic_configuration_;
    void* opaque_;
};

} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_CONFIGURATION_MOCK_H
