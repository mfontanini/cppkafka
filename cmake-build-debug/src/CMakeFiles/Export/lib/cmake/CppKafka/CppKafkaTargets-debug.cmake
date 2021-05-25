#----------------------------------------------------------------
# Generated CMake target import file for configuration "Debug".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "CppKafka::cppkafka" for configuration "Debug"
set_property(TARGET CppKafka::cppkafka APPEND PROPERTY IMPORTED_CONFIGURATIONS DEBUG)
set_target_properties(CppKafka::cppkafka PROPERTIES
  IMPORTED_LOCATION_DEBUG "${_IMPORT_PREFIX}/lib64/libcppkafka.so.0.3.1"
  IMPORTED_SONAME_DEBUG "libcppkafka.so.0.3.1"
  )

list(APPEND _IMPORT_CHECK_TARGETS CppKafka::cppkafka )
list(APPEND _IMPORT_CHECK_FILES_FOR_CppKafka::cppkafka "${_IMPORT_PREFIX}/lib64/libcppkafka.so.0.3.1" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
