cmake_minimum_required(VERSION 2.8.1)
project(cppkafka)

# Set the version number.
set(CPPKAFKA_VERSION_MAJOR 0)
set(CPPKAFKA_VERSION_MINOR 3)
set(CPPKAFKA_VERSION_REVISION 1)
set(CPPKAFKA_VERSION "${CPPKAFKA_VERSION_MAJOR}.${CPPKAFKA_VERSION_MINOR}.${CPPKAFKA_VERSION_REVISION}")
set(RDKAFKA_MIN_VERSION 0x00090400)

if(MSVC)
    # Don't always use Wall, since VC's /Wall is ridiculously verbose.
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /W3")

    # Disable VC secure checks, since these are not really issues
    add_definitions("-D_CRT_SECURE_NO_WARNINGS=1")
    add_definitions("-D_SCL_SECURE_NO_WARNINGS=1")
    add_definitions("-DNOGDI=1")
    add_definitions("-DNOMINMAX=1")
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wall")
endif()
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/")

# Set output directories
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/lib)

# Build output checks
option(CPPKAFKA_CMAKE_VERBOSE "Generate verbose output." OFF)
option(CPPKAFKA_BUILD_SHARED "Build cppkafka as a shared library." ON)
option(CPPKAFKA_DISABLE_TESTS "Disable build of cppkafka tests." OFF)
option(CPPKAFKA_DISABLE_EXAMPLES "Disable build of cppkafka examples." OFF)
option(CPPKAFKA_BOOST_STATIC_LIBS "Link with Boost static libraries." ON)
option(CPPKAFKA_BOOST_USE_MULTITHREADED "Use Boost multithreaded libraries." ON)
option(CPPKAFKA_RDKAFKA_STATIC_LIB "Link with Rdkafka static library." OFF)

# Disable output from find_package macro
if (NOT CPPKAFKA_CMAKE_VERBOSE)
    set(FIND_PACKAGE_QUIET QUIET)
endif()

if(CPPKAFKA_BUILD_SHARED)
    message(STATUS "Build will generate a shared library. "
            "Use CPPKAFKA_BUILD_SHARED=0 to perform a static build")
    set(CPPKAFKA_LIBRARY_TYPE SHARED)
else()
    message(STATUS "Build will generate a static library.")
    set(CPPKAFKA_LIBRARY_TYPE STATIC)
    add_definitions("-DCPPKAFKA_STATIC=1")
endif()

if (CPPKAFKA_RDKAFKA_STATIC_LIB)
    add_definitions("-DLIBRDKAFKA_STATICLIB")
endif()

# Look for Boost (just need boost.optional headers here)
find_package(Boost REQUIRED ${FIND_PACKAGE_QUIET})
find_package(RdKafka REQUIRED ${FIND_PACKAGE_QUIET})

if (Boost_FOUND)
    find_package(Boost COMPONENTS program_options ${FIND_PACKAGE_QUIET})
    set(Boost_USE_STATIC_LIBS ${CPPKAFKA_BOOST_STATIC_LIBS})
    set(Boost_USE_MULTITHREADED ${CPPKAFKA_BOOST_USE_MULTITHREADED})
    include_directories(${Boost_INCLUDE_DIRS})
    link_directories(${Boost_LIBRARY_DIRS})
    if (CPPKAFKA_CMAKE_VERBOSE)
        message(STATUS "Boost include dir: ${Boost_INCLUDE_DIRS}")
        message(STATUS "Boost library dir: ${Boost_LIBRARY_DIRS}")
        message(STATUS "Boost use static libs: ${Boost_USE_STATIC_LIBS}")
        message(STATUS "Boost is multi-threaded: ${CPPKAFKA_BOOST_USE_MULTITHREADED}")
        message(STATUS "Boost libraries: ${Boost_LIBRARIES}")
    endif()
endif()

add_subdirectory(src)
add_subdirectory(include)

# Examples target
if (NOT CPPKAFKA_DISABLE_EXAMPLES AND Boost_PROGRAM_OPTIONS_FOUND)
    add_subdirectory(examples)
else()
    message(STATUS "Disabling examples")
endif()

# Add a target to generate API documentation using Doxygen
find_package(Doxygen ${FIND_PACKAGE_QUIET})
if(DOXYGEN_FOUND)
    configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/docs/Doxyfile.in 
        ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile 
        @ONLY
    )
    add_custom_target(
        docs
        ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Generating API documentation with Doxygen" VERBATIM
    )
endif(DOXYGEN_FOUND)

if(NOT CPPKAFKA_DISABLE_TESTS)
    set(CATCH_ROOT ${CMAKE_SOURCE_DIR}/third_party/Catch2)
    if(EXISTS ${CATCH_ROOT}/CMakeLists.txt)
        set(CATCH_INCLUDE ${CATCH_ROOT}/single_include)
        enable_testing()
        add_subdirectory(tests)
    else()
        message(STATUS "Disabling tests because submodule Catch2 isn't checked out")
    endif()
else()
    message(STATUS "Disabling tests")
endif()

if(NOT TARGET uninstall)
    # Confiugure the uninstall script
    configure_file(
        "${CMAKE_CURRENT_SOURCE_DIR}/cmake/cmake_uninstall.cmake.in"
        "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake"
        IMMEDIATE @ONLY
    )

    # Add uninstall target
    add_custom_target(uninstall
        COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake)
endif()
