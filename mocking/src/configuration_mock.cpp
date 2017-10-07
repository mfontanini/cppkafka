#include <cppkafka/mocking/configuration_mock.h>

using std::string;
using std::default_delete;
using std::unordered_map;

namespace cppkafka {
namespace mocking {

ConfigurationMock::ConfigurationMock()
: default_topic_configuration_(nullptr, default_delete<ConfigurationMock>{}, Cloner{}) {

}

void ConfigurationMock::set(string key, string value) {
    options_[move(key)] = move(value);
}

size_t ConfigurationMock::has_key(const string& key) const {
    return options_.count(key);
}

string ConfigurationMock::get(const string& key) const {
    return options_.at(key);
}

void ConfigurationMock::set_delivery_report_callback(DeliveryReportCallback* callback) {
    set_callback(Callback::DeliveryReport, callback);
}

void ConfigurationMock::set_rebalance_callback(RebalanceCallback* callback) {
    set_callback(Callback::Rebalance, callback);
}

void ConfigurationMock::set_offset_commit_callback(OffsetCommitCallback* callback) {
    set_callback(Callback::OffsetCommit, callback);
}

void ConfigurationMock::set_error_callback(ErrorCallback* callback) {
    set_callback(Callback::Error, callback);
}

void ConfigurationMock::set_throttle_callback(ThrottleCallback* callback) {
    set_callback(Callback::Throttle, callback);
}

void ConfigurationMock::set_log_callback(LogCallback* callback) {
    set_callback(Callback::Log, callback);
}

void ConfigurationMock::set_stats_callback(StatsCallback* callback) {
    set_callback(Callback::Stats, callback);
}

void ConfigurationMock::set_socket_callback(SocketCallback* callback) {
    set_callback(Callback::Socket, callback);
}

void ConfigurationMock::set_partitioner_callback(PartitionerCallback* callback) {
    set_callback(Callback::Partitioner, callback);
}

void ConfigurationMock::set_default_topic_configuration(const ConfigurationMock& conf) {
    default_topic_configuration_.reset(Cloner{}(&conf));
}

void ConfigurationMock::set_opaque(void* opaque) {
    opaque_ = opaque;
}

ConfigurationMock::DeliveryReportCallback*
ConfigurationMock::get_delivery_report_callback() const {
    return get_callback<DeliveryReportCallback>(Callback::DeliveryReport);
}

ConfigurationMock::RebalanceCallback*
ConfigurationMock::get_rebalance_callback() const {
    return get_callback<RebalanceCallback>(Callback::Rebalance);
}

ConfigurationMock::OffsetCommitCallback*
ConfigurationMock::get_offset_commit_callback() const {
    return get_callback<OffsetCommitCallback>(Callback::OffsetCommit);
}

ConfigurationMock::ErrorCallback* ConfigurationMock::get_error_callback() const {
    return get_callback<ErrorCallback>(Callback::Error);
}

ConfigurationMock::ThrottleCallback* ConfigurationMock::get_throttle_callback() const {
    return get_callback<ThrottleCallback>(Callback::Throttle);
}

ConfigurationMock::LogCallback* ConfigurationMock::get_log_callback() const {
    return get_callback<LogCallback>(Callback::Log);
}

ConfigurationMock::StatsCallback* ConfigurationMock::get_stats_callback() const {
    return get_callback<StatsCallback>(Callback::Stats);
}

ConfigurationMock::SocketCallback* ConfigurationMock::get_socket_callback() const {
    return get_callback<SocketCallback>(Callback::Socket);
}

ConfigurationMock::PartitionerCallback* ConfigurationMock::get_partitioner_callback() const {
    return get_callback<PartitionerCallback>(Callback::Partitioner);
}

const ConfigurationMock* ConfigurationMock::get_default_topic_configuration() const {
    return default_topic_configuration_.get();
}

void* ConfigurationMock::get_opaque() const {
    return opaque_;
}

unordered_map<string, string> ConfigurationMock::get_options() const {
    return options_;
}

template <typename Functor>
void ConfigurationMock::set_callback(Callback type, Functor* ptr) {
    callbacks_[type] = reinterpret_cast<void*>(ptr);
}

template <typename Functor>
Functor* ConfigurationMock::get_callback(Callback type) const {
    auto iter = callbacks_.find(type);
    return iter == callbacks_.end() ? nullptr : reinterpret_cast<Functor*>(iter->second);
}

} // mocking
} // cppkafka
