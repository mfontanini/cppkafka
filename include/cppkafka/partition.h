#ifndef CPPKAFKA_PARTITION_H
#define CPPKAFKA_PARTITION_H

namespace cppkafka {

class Partition {
public:
    Partition();
    Partition(int partition);

    int get_partition() const; 
private:
    int partition_;
};

} // cppkafka

#endif // CPPKAFKA_PARTITION_H
