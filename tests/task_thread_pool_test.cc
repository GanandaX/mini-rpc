#include "TaskThreadPool.h"

#include <assert.h>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <iostream>
#include <stdexcept>

void task_thread_pool_executes_accepted_tasks_before_stop_returns_test() {
  uint8_t firstTaskCnt = 0;
  uint8_t secondTaskCnt = 0;
  uint8_t thirdTaskCnt = 0;

  std::thread::id callerThreadId = std::this_thread::get_id();

  TaskThreadPool threadPool(3);

  assert(threadPool.addTask([&]() {}) != TaskSubmitResult::kAccepted);

  threadPool.start();

  assert(threadPool.addTask([&]() {
    ++firstTaskCnt;
    assert(std::this_thread::get_id() != callerThreadId);
  }) == TaskSubmitResult::kAccepted);
  assert(threadPool.addTask([&]() {
    ++secondTaskCnt;
    assert(std::this_thread::get_id() != callerThreadId);
  }) == TaskSubmitResult::kAccepted);
  assert(threadPool.addTask([&]() {
    ++thirdTaskCnt;
    assert(std::this_thread::get_id() != callerThreadId);
  }) == TaskSubmitResult::kAccepted);

  threadPool.stop();

  assert(firstTaskCnt == 1);
  assert(secondTaskCnt == 1);
  assert(thirdTaskCnt == 1);
}

void task_thread_pool_drains_queued_tasks_during_stop_test() {
  std::thread::id callerThreadId = std::this_thread::get_id();

  uint8_t firstTaskCnt = 0;
  uint8_t secondTaskCnt = 0;

  std::promise<void> firstTaskStarted;
  auto firstTaskFuture = firstTaskStarted.get_future();
  std::promise<void> releaseFirstTask;
  auto releaseFirstFuture = releaseFirstTask.get_future();

  TaskThreadPool threadPool(1);
  threadPool.start();

  assert(threadPool.addTask([&]() {
    ++firstTaskCnt;
    assert(std::this_thread::get_id() != callerThreadId);

    firstTaskStarted.set_value();

    auto waitForRes =
        releaseFirstFuture.wait_for(std::chrono::milliseconds(1000));
    if (waitForRes != std::future_status::ready) {
      std::abort();
    }
  }) == TaskSubmitResult::kAccepted);
  assert(threadPool.addTask([&]() {
    ++secondTaskCnt;
    assert(std::this_thread::get_id() != callerThreadId);
  }) == TaskSubmitResult::kAccepted);

  std::thread another_thread([&]() {
    auto waitForRes = firstTaskFuture.wait_for(std::chrono::milliseconds(1000));
    if (waitForRes != std::future_status::ready) {
      std::abort();
    }

    auto start = std::chrono::steady_clock::now();

    while (true) {
      if (threadPool.addTask([]() {}) != TaskSubmitResult::kAccepted) {
        break;
      }
      if (std::chrono::steady_clock::now() - start >= std::chrono::seconds(1)) {
        std::abort();
      }
      std::this_thread::yield();
    }

    releaseFirstTask.set_value();
  });

  threadPool.stop();
  another_thread.join();

  assert(firstTaskCnt == 1);
  assert(secondTaskCnt == 1);
}

void task_thread_pool_continues_after_task_throws_test() {
  std::thread::id callerThreadId = std::this_thread::get_id();

  uint8_t firstTaskCnt = 0;
  uint8_t secondTaskCnt = 0;
  uint8_t thirdTaskCnt = 0;

  TaskThreadPool threadPool(1);
  threadPool.start();

  assert(threadPool.addTask([&]() {
    ++firstTaskCnt;
    assert(std::this_thread::get_id() != callerThreadId);

    throw 123;
  }) == TaskSubmitResult::kAccepted);

  assert(threadPool.addTask([&]() {
    ++secondTaskCnt;
    assert(std::this_thread::get_id() != callerThreadId);
    throw std::runtime_error("123");
  }) == TaskSubmitResult::kAccepted);

  assert(threadPool.addTask([&]() {
    ++thirdTaskCnt;
    assert(std::this_thread::get_id() != callerThreadId);
  }) == TaskSubmitResult::kAccepted);

  threadPool.stop();

  assert(firstTaskCnt == 1);
  assert(secondTaskCnt == 1);
  assert(thirdTaskCnt == 1);
}

void task_thread_pool_rejects_tasks_when_queue_is_full_test() {
  uint8_t firstTaskCnt = 0;
  uint8_t secondTaskCnt = 0;
  uint8_t thirdTaskCnt = 0;

  std::promise<void> firstTaskStarted;
  auto firstTaskFuture = firstTaskStarted.get_future();

  std::promise<void> releaseFirstTask;
  auto releaseFirstTaskFuture = releaseFirstTask.get_future();

  TaskThreadPool threadPool(1);
  threadPool.setMaxQueueTasks(1);
  threadPool.start();

  assert(threadPool.addTask([&]() {
    ++firstTaskCnt;
    firstTaskStarted.set_value();

    auto waitForRes = releaseFirstTaskFuture.wait_for(std::chrono::milliseconds(1000));
    if (waitForRes != std::future_status::ready) {
      std::abort();
    }
  }) == TaskSubmitResult::kAccepted);

  auto waitForRes = firstTaskFuture.wait_for(std::chrono::milliseconds(1000));
  if (waitForRes != std::future_status::ready) {
    std::abort();
  }

  assert(threadPool.addTask([&]() { ++secondTaskCnt; }) ==
         TaskSubmitResult::kAccepted);

  assert(threadPool.addTask([&]() { ++thirdTaskCnt; }) ==
         TaskSubmitResult::kQueueFull);

  releaseFirstTask.set_value();

  threadPool.stop();

  assert(firstTaskCnt == 1);
  assert(secondTaskCnt == 1);
  assert(thirdTaskCnt == 0);
}

int main() {
  task_thread_pool_executes_accepted_tasks_before_stop_returns_test();
  task_thread_pool_drains_queued_tasks_during_stop_test();
  task_thread_pool_continues_after_task_throws_test();
  task_thread_pool_rejects_tasks_when_queue_is_full_test();
}