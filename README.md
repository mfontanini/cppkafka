# cppkafka

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

# It's simple!

_cppkafka_'s API is simple to use. For example, this code creates a producer that writes a message
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

Note that finding _librdkafka_ will succeed iff there's an _include_ and _lib_
directories inside the specified path, including both the _rdkafka.h_ header
and the _librdkafka_ library file.

---

By default, a shared library will be built. If you want to perform a static build,
use the _CPPKAFKA_BUILD_SHARED_ parameter:

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

