#ifndef TASK_THREAD_POOL_H
#define TASK_THREAD_POOL_H

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>
#include <vector>

enum class TaskThreadPoolStatus { NOT_START, RUNNING, STOPPING, STOPPED };

enum class TaskSubmitResult { kAccepted, kNotRunning, kQueueFull, kEmptyTask };

class TaskThreadPool {
public:
  explicit TaskThreadPool(uint8_t threadCnt);
  ~TaskThreadPool();

  void start();

  /**
   * 拒绝新任务，执行完已接收任务，等待全部工作线程退出后返回
   */
  void stop();

  TaskSubmitResult addTask(std::function<void()> task);

  void setMaxQueueTasks(size_t maxQueueTasks);

private:
  void run();

private:
  TaskThreadPoolStatus status_;
  uint8_t threadCnt_;
  std::optional<size_t> maxQueueTasks_;
  std::mutex mutex_;
  std::condition_variable condition_;
  std::vector<std::thread> threads_;
  std::queue<std::function<void()>> tasks_;
};

#endif