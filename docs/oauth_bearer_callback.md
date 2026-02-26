# OAuth Bearer Token Refresh Callback

## Overview

The OAuth bearer token refresh callback allows you to implement custom OAuth bearer token generation and refresh logic in cppkafka. This is particularly useful for authentication mechanisms like AWS MSK IAM, Azure Event Hubs, or any custom OAuth implementation.

## API

### Setting the Callback

```cpp
using OAuthBearerTokenRefreshCallback = std::function<void(KafkaHandleBase& handle,
                                                           const std::string* oauthbearer_config)>;

Configuration& set_oauthbearer_token_refresh_callback(OAuthBearerTokenRefreshCallback callback);
```

### Getting the Callback

```cpp
const OAuthBearerTokenRefreshCallback& get_oauthbearer_token_refresh_callback() const;
```

## Usage

### Basic Setup

1. **Create a callback function** that generates or fetches OAuth tokens:

```cpp
void my_oauth_callback(KafkaHandleBase& handle, const std::string* oauthbearer_config) {
    // Check if config is provided
    if (oauthbearer_config) {
        // Parse config if needed
        // Use *oauthbearer_config to access the value
    }
    
    // Generate token
    std::string token = generate_my_token();
    int64_t expiry_ms = get_token_expiry();
    std::string principal = get_principal();
    
    // Set the token
    char errstr[512];
    rd_kafka_resp_err_t err = rd_kafka_oauthbearer_set_token(
        handle.get_handle(),
        token.c_str(),
        expiry_ms,
        principal.c_str(),
        nullptr, 0,
        errstr, sizeof(errstr)
    );
    
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_oauthbearer_set_token_failure(handle.get_handle(), errstr);
    }
}
```

2. **Configure Kafka with OAUTHBEARER**:

```cpp
Configuration config = {
    {"metadata.broker.list", "broker:9092"},
    {"group.id", "my-consumer"},
    {"sasl.mechanism", "OAUTHBEARER"},
    {"security.protocol", "SASL_SSL"}
};
```

3. **Set the callback**:

```cpp
config.set_oauthbearer_token_refresh_callback(my_oauth_callback);
```

4. **Create consumer or producer**:

```cpp
Consumer consumer(config);
// or
Producer producer(config);
```

## Callback Parameters

### KafkaHandleBase& handle
The Kafka handle (consumer or producer) requesting token refresh. Use `handle.get_handle()` to get the underlying `rd_kafka_t*` pointer for calling librdkafka functions.

### const std::string* oauthbearer_config
A pointer to the value of the `sasl.oauthbearer.config` configuration property. If the configuration property is not set, this will be `nullptr`. You can use this to pass custom parameters to your callback. Always check for `nullptr` before dereferencing.

## Callback Responsibilities

Your callback must either:

1. **Successfully set a token** using `rd_kafka_oauthbearer_set_token()`, or
2. **Report failure** using `rd_kafka_oauthbearer_set_token_failure()`

Failure to do either will result in authentication hanging.

## Complete Example: AWS MSK IAM

```cpp
#include <cppkafka/cppkafka.h>
#include <aws/core/auth/AWSCredentialsProvider.h>

void aws_msk_token_callback(KafkaHandleBase& handle, const std::string* config) {
    try {
        // Parse region from config if provided, otherwise use default
        std::string region = "us-east-1";
        if (config && !config->empty()) {
            // Parse config (e.g., "region=us-east-1")
            // Simplified parsing shown here
            if (config->find("region=") == 0) {
                region = config->substr(7);
            }
        }
        
        // Get AWS credentials
        auto provider = Aws::Auth::DefaultAWSCredentialsProviderChain();
        auto credentials = provider.GetAWSCredentials();
        
        // Generate MSK IAM token (simplified)
        std::string token = generate_msk_iam_token(credentials, region);
        int64_t expiry_ms = current_time_ms() + 300000; // 5 minutes
        
        char errstr[512];
        rd_kafka_resp_err_t err = rd_kafka_oauthbearer_set_token(
            handle.get_handle(),
            token.c_str(),
            expiry_ms,
            credentials.GetAWSAccessKeyId().c_str(),
            nullptr, 0,
            errstr, sizeof(errstr)
        );
        
        if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            rd_kafka_oauthbearer_set_token_failure(handle.get_handle(), errstr);
        }
    } catch (const std::exception& e) {
        rd_kafka_oauthbearer_set_token_failure(handle.get_handle(), e.what());
    }
}

int main() {
    Configuration config = {
        {"metadata.broker.list", "b-1.mycluster.kafka.us-east-1.amazonaws.com:9098"},
        {"security.protocol", "SASL_SSL"},
        {"sasl.mechanism", "OAUTHBEARER"},
        {"sasl.oauthbearer.config", "region=us-east-1"}
    };
    
    config.set_oauthbearer_token_refresh_callback(aws_msk_token_callback);
    
    Consumer consumer(config);
    consumer.subscribe({"my-topic"});
    
    // Process messages...
}
```

## When is the Callback Invoked?

The callback is invoked:

1. **On initial connection** - Before the first authentication attempt
2. **Before token expiry** - Automatically when the current token is about to expire
3. **On authentication failure** - If the broker rejects the current token

## Thread Safety

The callback may be invoked from librdkafka's internal threads. Ensure your callback is thread-safe if it accesses shared resources.

## Error Handling

Always handle errors in your callback:

```cpp
void safe_oauth_callback(KafkaHandleBase& handle, const std::string* config) {
    try {
        // Check if config is provided
        if (config) {
            // Use *config to access the configuration string
        }
        
        // Token generation logic
        std::string token = generate_token();
        
        char errstr[512];
        rd_kafka_resp_err_t err = rd_kafka_oauthbearer_set_token(
            handle.get_handle(),
            token.c_str(),
            expiry_ms,
            principal.c_str(),
            nullptr, 0,
            errstr, sizeof(errstr)
        );
        
        if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            rd_kafka_oauthbearer_set_token_failure(handle.get_handle(), errstr);
        }
    } catch (const std::exception& e) {
        // Always report failures
        rd_kafka_oauthbearer_set_token_failure(handle.get_handle(), e.what());
    } catch (...) {
        rd_kafka_oauthbearer_set_token_failure(handle.get_handle(), 
                                               "Unknown error generating token");
    }
}
```

## Background Token Refresh

For background token refresh (useful for long-lived consumers with low traffic):

```cpp
// Enable SASL queue for background callbacks
rd_kafka_conf_enable_sasl_queue(config.get_handle(), 1);

// Enable background SASL callbacks (if supported)
rd_kafka_sasl_background_callbacks_enable(consumer.get_handle());
```

This ensures tokens are refreshed even when the consumer is idle.

## See Also

- [librdkafka OAuth documentation](https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#authentication)
- [AWS MSK IAM authentication](https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html)
- [OAuth 2.0 Bearer Token Usage](https://tools.ietf.org/html/rfc6750)