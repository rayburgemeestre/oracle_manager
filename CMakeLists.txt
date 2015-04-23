cmake_minimum_required(VERSION 2.8.4)
project(oracle_manager)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES main.cpp)
add_executable(oracle_manager ${SOURCE_FILES} NewCppClass.cpp NewCppClass.h)

INCLUDE_DIRECTORIES(/opt/oracle/product/11.2.0/dbhome_1/rdbms/public)

LINK_DIRECTORIES(/opt/oracle/product/11.2.0/dbhome_1/lib)

SET(REQ_LIB
/opt/oracle/product/11.2.0/dbhome_1/lib/libagtsh.so
/opt/oracle/product/11.2.0/dbhome_1/lib/libclntsh.so
/opt/oracle/product/11.2.0/dbhome_1/lib/libclntsh.so.11.1
/opt/oracle/product/11.2.0/dbhome_1/lib/libocci.so
/opt/oracle/product/11.2.0/dbhome_1/lib/libocci.so.11.1
/opt/oracle/product/11.2.0/dbhome_1/lib/libociei.so
/opt/oracle/product/11.2.0/dbhome_1/lib/libocijdbc11.so
/opt/oracle/product/11.2.0/dbhome_1/lib/liborasdkbase.so.11.1
/opt/oracle/product/11.2.0/dbhome_1/lib/liborasdk.so.11.1
/opt/oracle/product/11.2.0/dbhome_1/lib/libsqora.so.11.1
/opt/oracle/product/11.2.0/dbhome_1/lib/liborasdkbase.so
/opt/oracle/product/11.2.0/dbhome_1/lib/libodm11.so
/opt/oracle/product/11.2.0/dbhome_1/lib/libnnz11.so
/opt/oracle/product/11.2.0/dbhome_1/lib/liborasdk.so
/opt/oracle/product/11.2.0/dbhome_1/lib/liborasdkbase.so)

TARGET_LINK_LIBRARIES(oracle_manager ${REQ_LIB})


