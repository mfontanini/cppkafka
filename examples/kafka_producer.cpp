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
using cppkafka::Partition;

namespace po = boost::program_options;

#ifndef CPPKAFKA_HAVE_ZOOKEEPER
    static_assert(false, "Examples require the zookeeper extension");
#endif

int main(int argc, char* argv[]) {
    string zookeeper_endpoint;
    string topic_name;
    int partition_value = -1;

    po::options_description options("Options");
    options.add_options()
        ("help,h",    "produce this help message")
        ("zookeeper", po::value<string>(&zookeeper_endpoint)->required(), 
                      "the zookeeper endpoint")
        ("topic",     po::value<string>(&topic_name)->required(),
                      "the topic in which to write to")
        ("partition", po::value<int>(&partition_value),
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

    // Get the partition we want to write to. If no partition is provided, this will be
    // an unassigned one
    Partition partition;
    if (partition_value != -1) {
        partition = partition_value;
    }

    // Construct the configuration
    Configuration config;
    config.set("zookeeper", zookeeper_endpoint);

    // Create the producer
    Producer producer(config);
    // Get the topic we want
    Topic topic = producer.get_topic(topic_name);

    // Now read lines and write them into kafka
    string line;
    while (getline(cin, line)) {
        // Write the string into the partition
        producer.produce(topic, partition, line);
    }
}
