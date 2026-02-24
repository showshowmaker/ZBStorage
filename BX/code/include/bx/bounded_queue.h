#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstddef>
#include <deque>
#include <mutex>

namespace bx {

template <typename T>
class BoundedBlockingQueue {
 public:
  explicit BoundedBlockingQueue(std::size_t capacity) : capacity_(capacity) {}

  bool Push(T value, std::uint64_t* wait_ns = nullptr) {
    auto wait_begin = std::chrono::steady_clock::now();
    std::unique_lock<std::mutex> lock(mu_);
    cv_not_full_.wait(lock, [&] { return closed_ || queue_.size() < capacity_; });
    if (closed_) {
      return false;
    }
    if (wait_ns != nullptr) {
      const auto now = std::chrono::steady_clock::now();
      *wait_ns += static_cast<std::uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(now - wait_begin).count());
    }
    queue_.push_back(std::move(value));
    std::size_t current_size = queue_.size();
    if (current_size > peak_size_) {
      peak_size_ = current_size;
    }
    cv_not_empty_.notify_one();
    return true;
  }

  bool Pop(T* out, std::uint64_t* wait_ns = nullptr) {
    auto wait_begin = std::chrono::steady_clock::now();
    std::unique_lock<std::mutex> lock(mu_);
    cv_not_empty_.wait(lock, [&] { return closed_ || !queue_.empty(); });
    if (queue_.empty()) {
      return false;
    }
    if (wait_ns != nullptr) {
      const auto now = std::chrono::steady_clock::now();
      *wait_ns += static_cast<std::uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(now - wait_begin).count());
    }
    *out = std::move(queue_.front());
    queue_.pop_front();
    cv_not_full_.notify_one();
    return true;
  }

  void Close() {
    {
      std::lock_guard<std::mutex> lock(mu_);
      closed_ = true;
    }
    cv_not_empty_.notify_all();
    cv_not_full_.notify_all();
  }

  std::size_t PeakSize() const {
    std::lock_guard<std::mutex> lock(mu_);
    return peak_size_;
  }

 private:
  const std::size_t capacity_;
  mutable std::mutex mu_;
  std::condition_variable cv_not_empty_;
  std::condition_variable cv_not_full_;
  std::deque<T> queue_;
  bool closed_ = false;
  std::size_t peak_size_ = 0;
};

}  // namespace bx
