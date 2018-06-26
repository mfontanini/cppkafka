#include <stdexcept>
#include <iostream>
#include <boost/program_options.hpp>
#include "cppkafka/utils/buffered_producer.h"
#include "cppkafka/configuration.h"

using std::string;
using std::exception;
using std::getline;
using std::cin;
using std::cout;
using std::endl;

using cppkafka::BufferedProducer;
using cppkafka::Configuration;
using cppkafka::Topic;
using cppkafka::MessageBuilder;
using cppkafka::Message;

namespace po = boost::program_options;

int main(int argc, char* argv[]) {
    string brokers;
    string topic_name;
    int partition_value = -1;

    po::options_description options("Options");
    options.add_options()
        ("help,h",      "produce this help message")
        ("brokers,b",   po::value<string>(&brokers)->required(), 
                        "the kafka broker list")
        ("topic,t",     po::value<string>(&topic_name)->required(),
                        "the topic in which to write to")
        ("partition,p", po::value<int>(&partition_value),
                        "the partition to write into (unassigned if not provided)")
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

    // Create a message builder for this topic
    MessageBuilder builder(topic_name);

    // Get the partition we want to write to. If no partition is provided, this will be
    // an unassigned one
    if (partition_value != -1) {
        builder.partition(partition_value);
    }

    // Construct the configuration
    Configuration config = {
        { "metadata.broker.list", brokers }
    };

    // Create the producer
    BufferedProducer<string> producer(config);

    // Set a produce success callback
    producer.set_produce_success_callback([](const Message& msg) {
        cout << "Successfully produced message with payload " << msg.get_payload() << endl;
    });
    // Set a produce failure callback
    producer.set_produce_failure_callback([](const Message& msg) {
        cout << "Failed to produce message with payload " << msg.get_payload() << endl;
        // Return false so we stop trying to produce this message
        return false;
    });

    cout << "Producing messages into topic " << topic_name << endl;

    // Now read lines and write them into kafka
    string line;
    while (getline(cin, line)) {
        // Set the payload on this builder
        builder.payload(line);

        // Add the message we've built to the buffered producer
        producer.add_message(builder);

        // Now flush so we:
        // * emit the buffered message
        // * poll the producer so we dispatch on delivery report callbacks and
        // therefore get the produce failure/success callbacks
        producer.flush();
    }
}
