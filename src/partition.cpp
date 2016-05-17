#include "partition.h"
#include <librdkafka/rdkafka.h>

namespace cppkafka {

Partition::Partition() 
: partition_(RD_KAFKA_PARTITION_UA) {

}

Partition::Partition(int partition) 
: partition_(partition) {

}

int Partition::get_partition() const {
    return partition_;
}

} // cppkafka
