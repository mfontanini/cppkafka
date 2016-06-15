#include <librdkafka/rdkafka.h>

int main() {
    int version = rd_kafka_version();
    int major = (version >> 32) & 0xff;
    int minor = (version >> 24) & 0xff;
    // We need version >= 0.9
    return (major > 0 || minor >= 9) ? 0 : 1;
}