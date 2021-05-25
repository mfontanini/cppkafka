
####### Expanded from @PACKAGE_INIT@ by configure_package_config_file() #######
####### Any changes to this file will be overwritten by the next CMake run ####
####### The input file was config.cmake.in                            ########

get_filename_component(PACKAGE_PREFIX_DIR "${CMAKE_CURRENT_LIST_DIR}/../../../" ABSOLUTE)

macro(set_and_check _var _file)
  set(${_var} "${_file}")
  if(NOT EXISTS "${_file}")
    message(FATAL_ERROR "File or directory ${_file} referenced by variable ${_var} does not exist !")
  endif()
endmacro()

macro(check_required_components _NAME)
  foreach(comp ${${_NAME}_FIND_COMPONENTS})
    if(NOT ${_NAME}_${comp}_FOUND)
      if(${_NAME}_FIND_REQUIRED_${comp})
        set(${_NAME}_FOUND FALSE)
      endif()
    endif()
  endforeach()
endmacro()

####################################################################################

include(CMakeFindDependencyMacro)

# Add FindRdKafka.cmake
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_LIST_DIR}")

set(RDKAFKA_MIN_VERSION_HEX "0x00090400")

# Find boost optional
find_dependency(Boost REQUIRED)

# Try to find the RdKafka configuration file if present.
# This will search default system locations as well as RdKafka_ROOT and RdKafka_DIR paths if specified.
find_package(RdKafka QUIET CONFIG)
set(RDKAFKA_TARGET_IMPORTS ${RdKafka_FOUND})
if (NOT RdKafka_FOUND)
    find_dependency(RdKafka REQUIRED MODULE)
endif()

include("${CMAKE_CURRENT_LIST_DIR}/CppKafkaTargets.cmake")

# Export 'CppKafka_ROOT'
set_and_check(CppKafka_ROOT "${PACKAGE_PREFIX_DIR}")

# Export 'CppKafka_INSTALL_INCLUDE_DIR'
set_and_check(CppKafka_INSTALL_INCLUDE_DIR "${PACKAGE_PREFIX_DIR}/include")

# Export 'CppKafka_INSTALL_LIB_DIR'
set_and_check(CppKafka_INSTALL_LIB_DIR "${PACKAGE_PREFIX_DIR}/lib64")

# Validate installed components
check_required_components("CppKafka")
