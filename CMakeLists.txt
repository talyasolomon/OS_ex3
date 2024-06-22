cmake_minimum_required(VERSION 3.16)
project(ex3)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)
include_directories(Barrier)

add_executable(ex3
        Atomic/atomicdemo.cpp
        Atomic/atomicdemo_multivar.cpp
        Barrier/Barrier.cpp
        Barrier/Barrier.h
        Barrier/barrierdemo.cpp
        "Sample Client/SampleClient.cpp"
        MapReduceClient.h
        MapReduceFramework.h)