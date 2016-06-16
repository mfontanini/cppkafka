#include <stdexcept>
#include <iostream>
#include <csignal>
#include <boost/program_options.hpp>
#include "cppkafka/consumer.h"
#include "cppkafka/configuration.h"

using std::string;
using std::exception;
using std::cout;
using std::endl;

using cppkafka::Consumer;
using cppkafka::Configuration;
using cppkafka::Message;

namespace po = boost::program_options;

#ifndef CPPKAFKA_HAVE_ZOOKEEPER
    static_assert(false, "Examples require the zookeeper extension");
#endif

bool running = true;

int main(int argc, char* argv[]) {
    string zookeeper_endpoint;
    string topic_name;
    string group_id;

    po::options_description options("Options");
    options.add_options()
        ("help,h",    "produce this help message")
        ("zookeeper", po::value<string>(&zookeeper_endpoint)->required(), 
                      "the zookeeper endpoint")
        ("topic",     po::value<string>(&topic_name)->required(),
                      "the topic in which to write to")
        ("group-id",  po::value<string>(&group_id)->required(),
                      "the consumer group id")
        ;

    po::variables_map vm;

    try {
        po::store(po::command_line_parser(argc, argv).options(options).run(), vm);
        po::notify(vm);
    }
    catch (exception& ex) {
        cout << "Error parsing options: " << ex.what() << endl;
        cout << endl;
        cout << options << endl;
        return 1;
    }

    // Stop processing on SIGINT
    signal(SIGINT, [](int) { running = false; });

    // Construct the configuration
    Configuration config;
    config.set("zookeeper", zookeeper_endpoint);
    config.set("group.id", group_id);
    // Disable auto commit
    config.set("enable.auto.commit", false);

    // Create the consumer
    Consumer consumer(config);

    // Subscribe to the topic
    consumer.subscribe({ topic_name });

    // Now read lines and write them into kafka
    while (running) {
        // Try to consume a message
        Message msg = consumer.poll();
        if (msg) {
            // If we managed to get a message
            if (msg.has_error()) {
                if (msg.get_error() != RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                    cout << "[+] Received error notification: " << msg.get_error_string() << endl;
                }
            }
            else {
                // Print the key (if any)
                if (msg.get_key()) {
                    cout << msg.get_key() << " -> ";
                }
                // Print the payload
                cout << msg.get_payload() << endl;
                // Now commit the message
                consumer.commit(msg);
            }
        }
    }
}
