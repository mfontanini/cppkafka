/*
 * OAuth Bearer Token Refresh Callback Example
 * 
 * This example demonstrates how to use the OAuth bearer token refresh callback
 * in cppkafka. This is useful for authentication mechanisms like AWS MSK IAM.
 */

#include <iostream>
#include <cppkafka/cppkafka.h>

using namespace cppkafka;

// Example token refresh callback
void oauth_token_refresh_callback(KafkaHandleBase& handle, const std::string* oauthbearer_config) {
    std::cout << "OAuth token refresh requested" << std::endl;
    if (oauthbearer_config) {
        std::cout << "Config: " << *oauthbearer_config << std::endl;
    } else {
        std::cout << "Config: not set" << std::endl;
    }
    
    // In a real implementation, you would:
    // 1. Parse the oauthbearer_config to get any necessary parameters
    // 2. Generate or fetch a new OAuth token
    // 3. Call rd_kafka_oauthbearer_set_token() with the new token
    //    or rd_kafka_oauthbearer_set_token_failure() if token generation fails
    
    // Example (simplified):
    std::string token = "your-generated-token";
    int64_t token_expiry_ms = 3600000; // 1 hour from now
    std::string principal = "your-principal";
    
    char errstr[512];
    rd_kafka_resp_err_t err = rd_kafka_oauthbearer_set_token(
        handle.get_handle(),
        token.c_str(),
        token_expiry_ms,
        principal.c_str(),
        nullptr, 0, // no extensions
        errstr, sizeof(errstr)
    );
    
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        std::cerr << "Failed to set OAuth token: " << errstr << std::endl;
        rd_kafka_oauthbearer_set_token_failure(handle.get_handle(), errstr);
    } else {
        std::cout << "OAuth token set successfully" << std::endl;
    }
}

int main() {
    // Create configuration
    Configuration config = {
        {"metadata.broker.list", "localhost:9092"},
        {"group.id", "example-consumer"},
        {"sasl.mechanism", "OAUTHBEARER"},
        {"security.protocol", "SASL_SSL"}
    };
    
    // Set the OAuth bearer token refresh callback
    config.set_oauthbearer_token_refresh_callback(oauth_token_refresh_callback);
    
    // Create consumer
    Consumer consumer(config);
    
    // Subscribe to topics
    consumer.subscribe({"test-topic"});
    
    std::cout << "Consumer created with OAuth callback" << std::endl;
    std::cout << "The callback will be invoked when token refresh is needed" << std::endl;
    
    // Poll for messages (the callback will be triggered as needed)
    while (true) {
        Message msg = consumer.poll();
        if (msg) {
            if (!msg.get_error()) {
                std::cout << "Received message: " << msg.get_payload() << std::endl;
            }
        }
    }
    
    return 0;
}