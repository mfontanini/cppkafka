# cppkafka
---
High level C++ wrapper for _rdkafka_

**Still in development!**

---

# Features

* _cppkafka_ is a high level C++ wrapper for _rdkafka_, aiming at allowing to use _rdkafka_ in a 
simple, less error prone way. 

* _cppkafka_ provides an API to produce messages as well as consuming messages, but the latter is 
only supported via the high level consumer API. _cppkafka_ requires **rdkakfa >= 0.9.1** in 
order to use it. Other wrapped functionalities are also provided, like fetching metadata, 
offsets, etc.

# It's simple!

_cppkafka_'s API is simple. For example, this code creates a producer writes a message into some 
partition:

```c++
#include <cppkafka/producer.h>

using namespace std;
using namespace cppkafka;

int main() {
    // Create the config
    Configuration config = {
        { "metadata.broker.list", "127.0.0.1:9092" }
    };

    // Create the producer
    Producer producer(config);

    // Get the topic we'll write into
    Topic topic = producer.get_topic("my_topic");

    // Produce a message!
    string message = "hey there!";
    producer.produce(topic, 0 /*partition*/, message);
}
```

# Compiling

In order to compile _cppkafka_ you need:

* _rdkafka >= 0.9.1_
* _CMake_
* A compiler with good C++11 support (e.g. gcc >= 4.8). This was tested successfully on
_g++ 4.8.3_. 
* The boost library. _cppkafka_ only requires boost.optional, which is a header only library,
so this doesn't add any additional runtime dependencies.

Now, in order to build, just run:

```Shell
mkdir build
cd build
cmake ..
make
```

# Using

If you want to use _cppkafka_, you'll need to link your application with:

* _cppkafka_
* _rdkafka_
