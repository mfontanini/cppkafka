#include <stdexcept>
#include <iostream>
#include <boost/program_options.hpp>
#include "cppkafka/producer.h"
#include "cppkafka/configuration.h"

using std::string;
using std::exception;
using std::getline;
using std::cin;
using std::cout;
using std::endl;

using cppkafka::Producer;
using cppkafka::Configuration;
using cppkafka::Topic;
using cppkafka::MessageBuilder;

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
    Producer producer(config);

    cout << "Producing messages into topic " << topic_name << endl;

    // Now read lines and write them into kafka
    string line;
    while (getline(cin, line)) {
        // Set the payload on this builder
        builder.payload(line);

        // Actually produce the message we've built
        producer.produce(builder);
    }
    
    // Flush all produced messages
    producer.flush();
}
