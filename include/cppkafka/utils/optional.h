#ifdef _USE_CPP17
#include<optional>
#else
#include <boost/optional.hpp>
#endif

namespace cppkafka
{
#ifdef _USE_CPP17
	using std::optional;
#else
	using boost::optional
#endif
}