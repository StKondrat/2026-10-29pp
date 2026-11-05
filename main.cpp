#include <condition_variable>
#include <cstddef>
#include <exception>
#include <functional>
#include <iostream>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

class ThreadPool
{
public:
  explicit ThreadPool(size_t count);
  ~ThreadPool();

  void submit(std::function< void() > task);
  void wait();
  void pause();
  void resume();

private:
  std::queue< std::function< void() > > tasks_;
  std::vector< std::thread > threads_;
  std::mutex mutex_;
  std::condition_variable ready_;
  std::condition_variable finished_;
  bool stopped_ = false;
  bool paused_ = false;
  size_t active_ = 0;

  void execute();
  void stop();
};

ThreadPool::ThreadPool(size_t count)
{
  if (count == 0)
  {
    throw std::invalid_argument("Thread count must be positive");
  }

  try
  {
    for (size_t i = 0; i < count; ++i)
    {
      threads_.emplace_back(&ThreadPool::execute, this);
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

void ThreadPool::submit(std::function< void() > task)
{
  {
    std::lock_guard< std::mutex > lock(mutex_);

    if (stopped_)
    {
      throw std::logic_error("Thread pool is stopped");
    }

    tasks_.push(std::move(task));
  }

  ready_.notify_one();
}

void ThreadPool::pause()
{
  std::lock_guard< std::mutex > lock(mutex_);
  paused_ = true;
}

void ThreadPool::resume()
{
  {
    std::lock_guard< std::mutex > lock(mutex_);
    paused_ = false;
  }

  ready_.notify_all();
}

void ThreadPool::wait()
{
  std::unique_lock< std::mutex > lock(mutex_);
  finished_.wait(lock, [this]()
  {
    return tasks_.empty() && (active_ == 0);
  });
}

void ThreadPool::execute()
{
  while (true)
  {
    std::function< void() > task;

    {
      std::unique_lock< std::mutex > lock(mutex_);
      ready_.wait(lock, [this]()
      {
        return stopped_ || (!paused_ && !tasks_.empty());
      });

      if (stopped_ && tasks_.empty())
      {
        return;
      }

      task = std::move(tasks_.front());
      tasks_.pop();
      ++active_;
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

    {
      std::lock_guard< std::mutex > lock(mutex_);
      --active_;

      if (tasks_.empty() && (active_ == 0))
      {
        finished_.notify_all();
      }
    }
  }
}

void ThreadPool::stop()
{
  {
    std::lock_guard< std::mutex > lock(mutex_);
    stopped_ = true;
    paused_ = false;
  }

  ready_.notify_all();

  for (std::thread& thread : threads_)
  {
    thread.join();
  }
}

int main()
{
  const size_t threadCount = 3;
  const size_t taskCount = 10;
  std::vector< size_t > results(taskCount);
  ThreadPool pool(threadCount);

  pool.pause();

  for (size_t i = 0; i < taskCount; ++i)
  {
    pool.submit([&results, i]()
    {
      results[i] = i * i;
    });
  }

  pool.resume();
  pool.wait();

  for (size_t result : results)
  {
    std::cout << result << '\n';
  }

  return 0;
}
