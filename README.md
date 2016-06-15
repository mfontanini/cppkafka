# cppkafka
---
High level C++ wrapper for _rdkafka_

**Still in development!**

---

# Features

* _cppkafka_ is a high level C++ wrapper for _rdkafka_, aiming at allowing to use _rdkafka_ in a simple, less error prone way. 

* _cppkafka_ provides an API to produce messages as well as consuming messages but the latter is only supported via the high level consumer API (introduced on kafka 0.9). Therefore, you need **rdkakfa >= 0.9** in order to use it. Other wrapped functionalities are also provided, like fetching metadata, offsets, etc.

* _cppkafka_ adds smooth **zookeeper** support: you only need to add a _"zookeeper"_ attribute to your configuration object and your producer/consumer will automatically load the initial broker list from it while also listening for broker list updates. 

# It's simple!

_cppkafka_'s API is simple. For example, this code creates a producer writes a message into some partition:

```c++
#include <cppkafka/producer.h>
#include <string>
using namespace cppkafka;
using namespace std;
int main() {
    // Create the config
    Configuration config;
    config.set("zookeeper", "127.0.0.1:2181");
    // Create the producer
    Producer producer(config);
    // Get the topic we'll write into
    Topic topic = producer.get_topic("my_topic");
    string message = "hey there!";
    // Produce a message!
    producer.produce(topic, 0 /*partition*/, message);
}
```

# Compiling

In order to compile _cppkafka_ you need:

* _rdkafka >= 0.9_
* Optionally _libzookeeper_. This is used by default, but you can disable it invoking cmake
using the `-DENABLE_ZOOKEEPER=0` argument.
* A compiler with good C++11 support. This was tested successfully on _g++ 4.8.3_. 
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
* _zookeeper_ if you enabled it during build time (it's on by default).
