#include <chrono>
#define CATCH_CONFIG_RUNNER
#include <catch.hpp>

using std::string;
using std::chrono::steady_clock;
using std::chrono::milliseconds;
using std::chrono::duration_cast;

using Catch::ConsoleReporter;
using Catch::ReporterConfig;
using Catch::ReporterPreferences;
using Catch::TestCaseInfo;
using Catch::TestCaseStats;
using Catch::Totals;
using Catch::Session;

std::vector<std::string> KAFKA_TOPICS = {"cppkafka_test1", "cppkafka_test2"};
int KAFKA_NUM_PARTITIONS = 3;

namespace cppkafka {

class InstantTestReporter : public ConsoleReporter {
public:
    using ClockType = steady_clock;

    InstantTestReporter(const ReporterConfig& config)
    : ConsoleReporter(config) {
    }

    static string getDescription() {
        return "Reports the tests' progress as they run";
    }

    ReporterPreferences getPreferences() const override {
        ReporterPreferences output;
        output.shouldRedirectStdOut = false;
        return output;
    }

    void testCaseStarting(const TestCaseInfo& info) override {
        ConsoleReporter::testCaseStarting(info);
        stream << "Running test \"" << info.name << "\" @ " << info.lineInfo << "\n";
        test_start_ts_ = ClockType::now();
    }

    void testCaseEnded(const TestCaseStats& stats) override {
        const Totals& totals = stats.totals;
        const size_t totalTestCases = totals.assertions.passed + totals.assertions.failed;
        const auto elapsed = ClockType::now() - test_start_ts_;
        stream << "Done. " << totals.assertions.passed << "/" << totalTestCases
               << " assertions succeeded in " <<  duration_cast<milliseconds>(elapsed).count()
               << "ms\n";
    }
private:
    ClockType::time_point test_start_ts_;
};

CATCH_REGISTER_REPORTER("instant", InstantTestReporter)

} // cppkafka

int main(int argc, char* argv[]) {
    Session session;

    int returnCode = session.applyCommandLine( argc, argv );
    if (returnCode != 0) {
        return returnCode;
    }
    if (session.configData().reporterNames.empty()) {
        // Set our reporter as the default one
        session.configData().reporterNames.emplace_back("instant");
    }

    int numFailed = session.run();
    return numFailed;
}
