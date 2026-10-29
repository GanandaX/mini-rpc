#include "TaskThreadPool.h"
#include <assert.h>
#include <exception>
#include <iostream>
#include <utility>

TaskThreadPool::TaskThreadPool(uint8_t threadCnt)
    : status_(TaskThreadPoolStatus::NOT_START), threadCnt_(threadCnt) {
  assert(threadCnt_ > 0);
}

TaskThreadPool::~TaskThreadPool() { stop(); }

void TaskThreadPool::start() {
  {
    std::unique_lock<std::mutex> locker(mutex_);
    try {
      if (status_ != TaskThreadPoolStatus::NOT_START) {
        return;
      }

      status_ = TaskThreadPoolStatus::RUNNING;

      for (uint8_t i = 0; i < threadCnt_; ++i) {
        threads_.emplace_back(&TaskThreadPool::run, this);
      }
    } catch (...) {
      status_ = TaskThreadPoolStatus::STOPPING;
      locker.unlock();

      condition_.notify_all();
      for (uint8_t i = 0; i < threads_.size(); ++i) {
        threads_[i].join();
      }
      threads_.clear();

      {
        std::unique_lock<std::mutex> locker(mutex_);
        status_ = TaskThreadPoolStatus::STOPPED;
      }
      throw;
    }
  }
}

void TaskThreadPool::stop() {
  {
    std::unique_lock<std::mutex> locker(mutex_);

    if (status_ != TaskThreadPoolStatus::RUNNING) {
      return;
    }

    status_ = TaskThreadPoolStatus::STOPPING;
  }

  condition_.notify_all();

  for (uint8_t i = 0; i < threads_.size(); ++i) {
    threads_[i].join();
  }
  threads_.clear();

  {
    std::unique_lock<std::mutex> locker(mutex_);
    status_ = TaskThreadPoolStatus::STOPPED;
  }
}

TaskSubmitResult TaskThreadPool::addTask(std::function<void()> task) {
  if (!task) {
    return TaskSubmitResult::kEmptyTask;
  }

  {
    std::unique_lock<std::mutex> locker(mutex_);

    if (status_ != TaskThreadPoolStatus::RUNNING) {
      return TaskSubmitResult::kNotRunning;
    }

    if (maxQueueTasks_.has_value() && tasks_.size() >= maxQueueTasks_.value()) {
      return TaskSubmitResult::kQueueFull;
    }

    tasks_.emplace(std::move(task));
  }

  condition_.notify_one();
  return TaskSubmitResult::kAccepted;
}

void TaskThreadPool::setMaxQueueTasks(size_t maxQueueTasks) {
  assert(maxQueueTasks > 0);

  {
    std::unique_lock<std::mutex> locker(mutex_);

    if (status_ != TaskThreadPoolStatus::NOT_START || maxQueueTasks < 1) {
      return;
    }

    maxQueueTasks_ = maxQueueTasks;
  }
}

void TaskThreadPool::run() {
  while (true) {
    std::function<void()> task;

    {
      std::unique_lock<std::mutex> locker(mutex_);
      condition_.wait(locker, [&]() -> bool {
        return !tasks_.empty() || status_ != TaskThreadPoolStatus::RUNNING;
      });

      if (tasks_.empty()) {
        if (status_ != TaskThreadPoolStatus::RUNNING) {
          break;
        }
        continue;
      }

      task = std::move(tasks_.front());
      tasks_.pop();
    }

    try {
      task();
    } catch (const std::exception &e) {
      std::cerr << "task threw an exception: " << e.what() << std::endl;
    } catch (...) {
      std::cerr << "task threw an unknown exception" << std::endl;
    }
  }
}