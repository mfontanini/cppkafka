#include <stdexcept>
#include <iostream>
#include <boost/program_options.hpp>
#include "cppkafka/producer.h"
#include "cppkafka/configuration.h"
#include "cppkafka/group_information.h"
#include "cppkafka/topic.h"

using std::string;
using std::exception;
using std::vector;
using std::cout;
using std::endl;

using cppkafka::Producer;
using cppkafka::Exception;
using cppkafka::Configuration;
using cppkafka::Topic;
using cppkafka::GroupInformation;
using cppkafka::GroupMemberInformation;
using cppkafka::MemberAssignmentInformation;

namespace po = boost::program_options;

int main(int argc, char* argv[]) {
    string brokers;
    string group_id;
    bool show_assignment = false;

    po::options_description options("Options");
    options.add_options()
        ("help,h",       "produce this help message")
        ("brokers,b",    po::value<string>(&brokers)->required(), 
                         "the kafka broker list")
        ("group-id,g",   po::value<string>(&group_id),
                         "only fetch consumer group information for the specified one")
        ("assignment,a", po::value<bool>(&show_assignment)->implicit_value(true),
                         "show topic/partition assignment for each consumer group")
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
        // Disable auto commit
        { "enable.auto.commit", false }
    };

    try {
        // Construct a producer
        Producer producer(config);

        // Fetch the group information
        vector<GroupInformation> groups = [&]() {
            if (!group_id.empty()) {
                return vector<GroupInformation>{producer.get_consumer_group(group_id)};
            }
            else {
                return producer.get_consumer_groups();
            }
        }();

        if (groups.empty()) {
            cout << "Found no consumers" << endl;
            return 0;
        }
        cout << "Found the following consumers: " << endl;
        for (const GroupInformation& group : groups) {
            cout << "* \"" << group.get_name() << "\" having the following (" <<
                    group.get_members().size() << ") members: " << endl;
            for (const GroupMemberInformation& info : group.get_members()) {
                cout << "    - " << info.get_member_id() << " @ " << info.get_client_host();
                if (show_assignment) {
                    MemberAssignmentInformation assignment(info.get_member_assignment());
                    cout << " has assigned: " << assignment.get_topic_partitions();
                }
                cout << endl;
            }
            cout << endl;
        }
    }
    catch (const Exception& ex) {
        cout << "Error fetching group information: " << ex.what() << endl;
    }
}