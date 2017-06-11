#include <stdexcept>
#include <iostream>
#include <csignal>
#include <boost/program_options.hpp>
#include "cppkafka/consumer.h"
#include "cppkafka/configuration.h"
#include "cppkafka/utils/consumer_dispatcher.h"

using std::string;
using std::exception;
using std::cout;
using std::endl;
using std::function;

using cppkafka::Consumer;
using cppkafka::ConsumerDispatcher;
using cppkafka::Configuration;
using cppkafka::Message;
using cppkafka::TopicPartition;
using cppkafka::TopicPartitionList;
using cppkafka::Error;

namespace po = boost::program_options;

function<void()> on_signal;

void signal_handler(int) {
    on_signal();
}

// This example uses ConsumerDispatcher, a simple synchronous wrapper over a Consumer
// to allow processing messages using pattern matching rather than writing a loop
// and check if there's a message, if there's an error, etc. 
int main(int argc, char* argv[]) {
    string brokers;
    string topic_name;
    string group_id;

    po::options_description options("Options");
    options.add_options()
        ("help,h",     "produce this help message")
        ("brokers,b",  po::value<string>(&brokers)->required(), 
                       "the kafka broker list")
        ("topic,t",    po::value<string>(&topic_name)->required(),
                       "the topic in which to write to")
        ("group-id,g", po::value<string>(&group_id)->required(),
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

    // Construct the configuration
    Configuration config = {
        { "metadata.broker.list", brokers },
        { "group.id", group_id },
        // Disable auto commit
        { "enable.auto.commit", false }
    };

    // Create the consumer
    Consumer consumer(config);

    // Print the assigned partitions on assignment
    consumer.set_assignment_callback([](const TopicPartitionList& partitions) {
        cout << "Got assigned: " << partitions << endl;
    });

    // Print the revoked partitions on revocation
    consumer.set_revocation_callback([](const TopicPartitionList& partitions) {
        cout << "Got revoked: " << partitions << endl;
    });

    // Subscribe to the topic
    consumer.subscribe({ topic_name });

    cout << "Consuming messages from topic " << topic_name << endl;

    // Create a consumer dispatcher
    ConsumerDispatcher dispatcher(consumer);

    // Stop processing on SIGINT
    on_signal = [&]() {
        dispatcher.stop();
    };
    signal(SIGINT, signal_handler);

    // Now run the dispatcher, providing a callback to handle messages, one to handle
    // errors and another one to handle EOF on a partition
    dispatcher.run(
        // Callback executed whenever a new message is consumed
        [&](Message msg) {
            // Print the key (if any)
            if (msg.get_key()) {
                cout << msg.get_key() << " -> ";
            }
            // Print the payload
            cout << msg.get_payload() << endl;
            // Now commit the message
            consumer.commit(msg);
        },
        // Whenever there's an error (other than the EOF soft error)
        [](Error error) {
            cout << "[+] Received error notification: " << error << endl;
        },
        // Whenever EOF is reached on a partition, print this
        [](ConsumerDispatcher::EndOfFile, const TopicPartition& topic_partition) {
            cout << "Reched EOF on partition " << topic_partition << endl;
        }
    );
}
