cmake_minimum_required(VERSION 3.9.2)
project(CppKafka)
if (${CMAKE_VERSION} VERSION_GREATER_EQUAL "3.12.0")
    # Use <package>_ROOT variable to find configuration files
    cmake_policy(SET CMP0074 NEW)
endif()

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

# Set the version number.
set(CPPKAFKA_VERSION_MAJOR 0)
set(CPPKAFKA_VERSION_MINOR 4)
set(CPPKAFKA_VERSION_REVISION 0)
set(CPPKAFKA_VERSION "${CPPKAFKA_VERSION_MAJOR}.${CPPKAFKA_VERSION_MINOR}.${CPPKAFKA_VERSION_REVISION}")
set(RDKAFKA_MIN_VERSION "0.9.4")
set(RDKAFKA_MIN_VERSION_HEX 0x00090400)

if (NOT CMAKE_CXX_FLAGS)
    # Set default compile flags for the project
    if(MSVC)
        # Don't always use Wall, since VC's /Wall is ridiculously verbose.
        set(CMAKE_CXX_FLAGS "/W3")

        # Disable VC secure checks, since these are not really issues
        add_definitions("-D_CRT_SECURE_NO_WARNINGS=1")
        add_definitions("-D_SCL_SECURE_NO_WARNINGS=1")
        add_definitions("-DNOGDI=1")
        add_definitions("-DNOMINMAX=1")
    else()
        set(CMAKE_CXX_FLAGS "-std=c++11 -Wall")
    endif()
endif()

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
option(CPPKAFKA_EXPORT_PKGCONFIG "Generate 'cppkafka.pc' file" ON)
option(CPPKAFKA_EXPORT_CMAKE_CONFIG "Generate CMake config, target and version files." ON)

# Add FindRdKafka.cmake
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/")

if (NOT CPPKAFKA_CONFIG_DIR)
    set(CPPKAFKA_CONFIG_DIR lib/cmake/${PROJECT_NAME})
endif()

# Maintain previous compatibility
if (RDKAFKA_ROOT_DIR)
    set(RdKafka_ROOT ${RDKAFKA_ROOT_DIR})
elseif (RDKAFKA_ROOT)
    set(RdKafka_ROOT ${RDKAFKA_ROOT})
endif()

if (RdKafka_ROOT)
    if (NOT IS_ABSOLUTE ${RdKafka_ROOT})
        set(RdKafka_ROOT "${CMAKE_SOURCE_DIR}/${RdKafka_ROOT}")
    endif()
endif()

if (RDKAFKA_DIR)
    set(RdKafka_DIR ${RDKAFKA_DIR}) # For older versions of find_package
    if (NOT IS_ABSOLUTE ${RdKafka_ROOT})
        set(RdKafka_DIR "${CMAKE_SOURCE_DIR}/${RdKafka_DIR}")
    endif()
endif()

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
    set(CMAKE_POSITION_INDEPENDENT_CODE ON)
endif()

if (CPPKAFKA_RDKAFKA_STATIC_LIB)
    add_definitions("-DLIBRDKAFKA_STATICLIB")
endif()

if (NOT CPPKAFKA_CONFIG_DIR)
    set(CPPKAFKA_CONFIG_DIR lib/cmake/${PROJECT_NAME})
endif()

if (NOT CPPKAFKA_PKGCONFIG_DIR)
    set(CPPKAFKA_PKGCONFIG_DIR share/pkgconfig)
endif()

# Look for Boost (just need boost.optional headers here)
find_package(Boost REQUIRED ${FIND_PACKAGE_QUIET})

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

# Try to find the RdKafka configuration file if present.
# This will search default system locations as well as RdKafka_ROOT and RdKafka_Dir paths if specified.
find_package(RdKafka ${FIND_PACKAGE_QUIET} CONFIG)
set(RDKAFKA_TARGET_IMPORTS ${RdKafka_FOUND})
if (NOT RdKafka_FOUND)
    message(STATUS "RdKafkaConfig.cmake not found. Attempting to find module instead...")
    find_package(RdKafka REQUIRED ${FIND_PACKAGE_QUIET} MODULE)
    if (NOT RdKafka_FOUND)
        message(FATAL_ERROR "RdKafka module not found. Please set RDKAFKA_ROOT to the install path or RDKAFKA_DIR pointing to the RdKafka configuration file location.")
    else()
        message(STATUS "RdKafka module found.")
    endif()
else()
    message(STATUS "RdKafka configuration file found: ${RdKafka_CONFIG}")
endif()

add_subdirectory(src)
add_subdirectory(include/cppkafka)

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
