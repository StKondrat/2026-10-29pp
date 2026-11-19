#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <exception>
#include <functional>
#include <future>
#include <iostream>
#include <list>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

class ThreadPool
{
public:
  using Task = std::function< void() >;

  explicit ThreadPool(std::size_t threadCount, std::size_t batchSize = defaultBatchSize_);
  ~ThreadPool();

  void submit(Task task);
  void pause();
  void resume();
  void wait();

private:
  using RWLock = std::shared_timed_mutex;

  struct TaskQueue
  {
    std::list< Task > tasks;
    RWLock mutex;
  };

  static constexpr std::size_t defaultBatchSize_ = 4;
  TaskQueue globalQueue_;
  std::vector< TaskQueue > localQueues_;
  const std::size_t batchSize_;
  RWLock stateMutex_;
  std::condition_variable_any ready_;
  std::condition_variable_any finished_;
  std::atomic< std::size_t > queued_{0};
  std::size_t outstanding_ = 0;
  bool stopped_ = false;
  bool paused_ = false;
  std::vector< std::thread > threads_;

  bool tryTakeTask(TaskQueue& local, Task& task);
  bool tryTakeBatch(TaskQueue& source, TaskQueue& local, Task& task, bool steal);
  void execute(std::size_t worker);
  void stop();
};

ThreadPool::ThreadPool(std::size_t threadCount, std::size_t batchSize):
  localQueues_(threadCount),
  batchSize_(batchSize)
{
  if ((threadCount == 0) || (batchSize == 0))
  {
    throw std::invalid_argument("Thread count and batch size must be positive");
  }

  threads_.reserve(threadCount);

  try
  {
    for (std::size_t i = 0; i < threadCount; ++i)
    {
      threads_.emplace_back(&ThreadPool::execute, this, i);
    }
  }
  catch (...)
  {
    stop();
    throw;
  }
}

ThreadPool::~ThreadPool()
{
  stop();
}

void ThreadPool::submit(Task task)
{
  {
    std::unique_lock< RWLock > lock(stateMutex_);

    if (stopped_)
    {
      throw std::logic_error("Thread pool is stopped");
    }

    std::unique_lock< RWLock > queueLock(globalQueue_.mutex);
    globalQueue_.tasks.push_back(std::move(task));
    ++queued_;
    ++outstanding_;
  }

  ready_.notify_one();
}

void ThreadPool::pause()
{
  std::unique_lock< RWLock > lock(stateMutex_);
  paused_ = true;
}

void ThreadPool::resume()
{
  {
    std::unique_lock< RWLock > lock(stateMutex_);
    paused_ = false;
  }

  ready_.notify_all();
}

void ThreadPool::wait()
{
  std::shared_lock< RWLock > lock(stateMutex_);

  finished_.wait(lock, [this]()
  {
    return outstanding_ == 0;
  });
}

bool ThreadPool::tryTakeTask(TaskQueue& local, Task& task)
{
  {
    std::unique_lock< RWLock > lock(local.mutex);

    if (!local.tasks.empty())
    {
      task = std::move(local.tasks.front());
      local.tasks.pop_front();
      return true;
    }
  }

  if (tryTakeBatch(globalQueue_, local, task, false))
  {
    return true;
  }

  for (TaskQueue& source : localQueues_)
  {
    if ((&source != &local) && tryTakeBatch(source, local, task, true))
    {
      return true;
    }
  }

  return false;
}

bool ThreadPool::tryTakeBatch(TaskQueue& source, TaskQueue& local, Task& task, bool steal)
{
  {
    std::shared_lock< RWLock > lock(source.mutex);

    if (source.tasks.empty())
    {
      return false;
    }
  }

  std::unique_lock< RWLock > sourceLock(source.mutex, std::defer_lock);
  std::unique_lock< RWLock > localLock(local.mutex, std::defer_lock);
  std::lock(sourceLock, localLock);

  if (source.tasks.empty())
  {
    return false;
  }

  if (steal)
  {
    const std::size_t size = source.tasks.size();
    const std::size_t count = size - size / 2;
    auto begin = source.tasks.end();

    for (std::size_t i = 0; i < count; ++i)
    {
      --begin;
    }

    local.tasks.splice(local.tasks.end(), source.tasks, begin, source.tasks.end());
  }
  else
  {
    const std::size_t count = std::min(batchSize_, source.tasks.size());
    auto end = source.tasks.begin();

    for (std::size_t i = 0; i < count; ++i)
    {
      ++end;
    }

    local.tasks.splice(local.tasks.end(), source.tasks, source.tasks.begin(), end);
  }

  task = std::move(local.tasks.front());
  local.tasks.pop_front();
  return true;
}

void ThreadPool::execute(std::size_t worker)
{
  TaskQueue& local = localQueues_[worker];

  while (true)
  {
    Task task;

    {
      std::shared_lock< RWLock > lock(stateMutex_);

      ready_.wait(lock, [this]()
      {
        return stopped_ || (!paused_ && (queued_ != 0));
      });

      if (stopped_ && (queued_ == 0))
      {
        return;
      }

      if (!tryTakeTask(local, task))
      {
        continue;
      }

      --queued_;
    }

    try
    {
      task();
    }
    catch (const std::exception& error)
    {
      std::cerr << "Task failed: " << error.what() << '\n';
    }
    catch (...)
    {
      std::cerr << "Task failed: unknown exception\n";
    }

    task = nullptr;

    {
      std::unique_lock< RWLock > lock(stateMutex_);
      --outstanding_;

      if (outstanding_ == 0)
      {
        finished_.notify_all();
      }
    }
  }
}

void ThreadPool::stop()
{
  {
    std::unique_lock< RWLock > lock(stateMutex_);
    stopped_ = true;
  }

  ready_.notify_all();

  for (std::thread& thread : threads_)
  {
    thread.join();
  }
}

int main()
{
  const std::size_t threadCount = 2;
  const std::vector< int > durations{180, 25, 50, 75};
  const std::size_t lastTask = durations.size() - 1;
  const std::chrono::seconds timeout(5);
  const auto releaseTimeout = timeout + timeout;
  std::vector< std::thread::id > taskThreads(durations.size());
  std::promise< void > releaseLongTask;
  const auto release = releaseLongTask.get_future().share();
  std::promise< void > shortTaskDone;
  auto shortResult = shortTaskDone.get_future();
  ThreadPool pool(threadCount, durations.size());

  pool.pause();

  for (std::size_t i = 0; i < durations.size(); ++i)
  {
    pool.submit([&, i]()
    {
      taskThreads[i] = std::this_thread::get_id();
      std::this_thread::sleep_for(std::chrono::milliseconds(durations[i]));

      if (i == 0)
      {
        release.wait_for(releaseTimeout);
      }

      if (i == lastTask)
      {
        shortTaskDone.set_value();
      }
    });
  }

  pool.resume();

  const bool shortFinished = shortResult.wait_for(timeout) == std::future_status::ready;
  releaseLongTask.set_value();
  pool.wait();

  for (std::size_t i = 0; i < durations.size(); ++i)
  {
    std::cout << "Task " << i << ": " << durations[i] << " ms, thread " << taskThreads[i] << '\n';
  }

  if (!shortFinished || (taskThreads[0] == taskThreads[lastTask]))
  {
    std::cerr << "Work stealing test failed\n";
    return 1;
  }

  std::cout << "Work stealing test passed\n";
  return 0;
}
