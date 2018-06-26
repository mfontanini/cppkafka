#include <stdexcept>
#include <iostream>
#include <boost/program_options.hpp>
#include "cppkafka/producer.h"
#include "cppkafka/configuration.h"
#include "cppkafka/metadata.h"
#include "cppkafka/topic.h"

using std::string;
using std::exception;
using std::cout;
using std::endl;

using cppkafka::Producer;
using cppkafka::Exception;
using cppkafka::Configuration;
using cppkafka::Topic;
using cppkafka::Metadata;
using cppkafka::TopicMetadata;
using cppkafka::BrokerMetadata;

namespace po = boost::program_options;

int main(int argc, char* argv[]) {
    string brokers;

    po::options_description options("Options");
    options.add_options()
        ("help,h",     "produce this help message")
        ("brokers,b",  po::value<string>(&brokers)->required(), 
                       "the kafka broker list")
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
    };

    try {
        // Construct a producer
        Producer producer(config);

        // Fetch the metadata
        Metadata metadata = producer.get_metadata();

        // Iterate over brokers
        cout << "Found the following brokers: " << endl;
        for (const BrokerMetadata& broker : metadata.get_brokers()) {
            cout << "* " << broker.get_host() << endl;
        }
        cout << endl;

        // Iterate over topics
        cout << "Found the following topics: " << endl;
        for (const TopicMetadata& topic : metadata.get_topics()) {
            cout << "* " << topic.get_name() << ": " << topic.get_partitions().size()
                 << " partitions" << endl;
        }
    }
    catch (const Exception& ex) {
        cout << "Error fetching metadata: " << ex.what() << endl;
    }
}