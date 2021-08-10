 #ifdef _USE_CPP17
        #include<any>
    #else
        #include <boost/any.hpp>
    #endif

namespace cppkafka
{
    #ifdef _USE_CPP17
        using any = std::any;
    #else
        using any = boost::any;
    #endif
}