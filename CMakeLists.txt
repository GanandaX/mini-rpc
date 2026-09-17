cmake_minimum_required(VERSION 3.20)

project(mini_rpc LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)


add_library(mini_rpc
    src/RpcMessage.cc
    src/RpcServer.cc
    src/RpcClient.cc
)

target_include_directories(mini_rpc PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/src/include
)

add_subdirectory(
    ${CMAKE_CURRENT_SOURCE_DIR}/../mini_muduo
    ${CMAKE_CURRENT_BINARY_DIR}/mini_muduo_build
)

target_link_libraries(mini_rpc PUBLIC mini_muduo)

target_compile_options(mini_rpc PRIVATE
        -Wall
        -Wextra
        -Wpedantic
)

function(add_example name)
        add_executable(${name} examples/${name}.cc)
        target_link_libraries(${name} PRIVATE mini_rpc)
        target_compile_options(${name} PRIVATE -Wall -Wextra -Wpedantic)
endfunction()

add_example(rpc_echo_example)


include(CTest)
if (BUILD_TESTING)
    function(add_mini_test name) 
            add_executable(${name} tests/${name}.cc)
            target_link_libraries(${name} PRIVATE mini_rpc)
            add_test(NAME ${name} COMMAND ${name})
    endfunction()

    add_mini_test(rpc_message_test)
    add_mini_test(rpc_server_test)
endif()