# cppkafka: high level C++ wrapper for _rdkafka_

[![Build status](https://travis-ci.org/mfontanini/cppkafka.svg?branch=master)](https://travis-ci.org/mfontanini/cppkafka) 

_cppkafka_ allows C++ applications to consume and produce messages using the Apache Kafka
protocol. The library is built on top of [_librdkafka_](https://github.com/edenhill/librdkafka), 
and provides a high level API that uses modern C++ features to make it easier to write code
while keeping the wrapper's performance overhead to a minimum.

# Features

* _cppkafka_ is a high level C++ wrapper for _rdkafka_, aiming to allow using _rdkafka_ in a 
simple, less error prone way. 

* _cppkafka_ provides an API to produce messages as well as consuming messages, but the latter is 
only supported via the high level consumer API. _cppkafka_ requires **rdkafka >= 0.9.4** in 
order to use it. Other wrapped functionalities are also provided, like fetching metadata, 
offsets, etc.

* _cppkafka_ tries to add minimal overhead over _librdkafka_. A very thin wrapper for _librdkafka_
messages is used for consumption so there's virtually no overhead at all.

# It's simple!

_cppkafka_'s API is simple to use. For example, this code creates a producer that writes a message
into some partition:

```c++
#include <cppkafka/cppkafka.h>

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
    producer.flush();
}
```

# Compiling

In order to compile _cppkafka_ you need:

* _librdkafka >= 0.9.4_
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

## CMake options

If you have installed _librdkafka_ on a non standard directory, you can use the
`RDKAFKA_ROOT_DIR` cmake parameter when configuring the project:

```Shell
cmake .. -DRDKAFKA_ROOT_DIR=/some/other/dir
```

Note that the `RDKAFKA_ROOT_DIR` must contain the following structure:

```Shell
${RDKAFKA_ROOT_DIR}/
                   |
                   + include/librdkafka/rdkafka.h
                   |
                   + lib/librdkafka.a
```

By default, a shared library will be built. If you want to perform a static build,
use the `CPPKAFKA_BUILD_SHARED` parameter:

```Shell
cmake .. -DCPPKAFKA_BUILD_SHARED=0
```

# Using

If you want to use _cppkafka_, you'll need to link your application with:

* _cppkafka_
* _rdkafka_

# Documentation

You can generate the documentation by running `make docs` inside the build directory. This requires
_Doxygen_ to be installed. The documentation will be written in html format at
`<build-dir>/docs/html/`.

Make sure to check the [wiki](https://github.com/mfontanini/cppkafka/wiki) which includes
some documentation about the project and some of its features.

