# cppkafka
---
High level C++ wrapper for _rdkafka_
---

# Features

* _cppkafka_ is a high level C++ wrapper for _rdkafka_, aiming to allow using _rdkafka_ in a 
simple, less error prone way. 

* _cppkafka_ provides an API to produce messages as well as consuming messages, but the latter is 
only supported via the high level consumer API. _cppkafka_ requires **rdkakfa >= 0.9.4** in 
order to use it. Other wrapped functionalities are also provided, like fetching metadata, 
offsets, etc.

* _cppkafka_ tries to add minimal overhead over _librdkafka_. A very thin wrapper for _librdkafka_
messages is used for consumption so there's virtually no overhead at all.

**Note**: _cppkafka_ is under development so the API might change through time. Once there's an
actual release, it should become more stable.

# It's simple!

_cppkafka_'s API is simple. For example, this code creates a producer that writes a message
into some partition:

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

    // Produce a message!
    string message = "hey there!";
    producer.produce(MessageBuilder("my_topic").partition(0).payload(message));
}
```

# Compiling

In order to compile _cppkafka_ you need:

* _rdkafka >= 0.9.4_
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

# Documentation

You can generate the documentation by running `make docs` inside the build directory. This requires
_Doxygen_ to be installed. The documentation will be written in html format at
`<build-dir>/docs/html/`.
