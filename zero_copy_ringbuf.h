#ifndef ZERO_COPY_RINGBUF_H
#define ZERO_COPY_RINGBUF_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <sys/mman.h>
#include <unistd.h>
#include <stdexcept>

template<typename T, size_t Capacity>
class ZeroCopyRingBuf {
    static_assert(std::is_trivially_copyable<T>::value, "T must be trivially copyable");
public:
    ZeroCopyRingBuf() : write_idx_(0), read_idx_(0) {
        // 预置所有槽位为未使用状态（这里无需特殊处理）
    }

    // 生产者：获取可写的槽位指针（如果队列不满）
    T* get_write_slot() {
        size_t w = write_idx_.load(std::memory_order_relaxed);
        size_t r = read_idx_.load(std::memory_order_acquire);
        if (next(w) == r) return nullptr; // full
        return &buffer_[w];
    }

    // 生产者：提交当前写槽位（必须紧跟在 get_write_slot 之后）
    void commit_write() {
        size_t w = write_idx_.load(std::memory_order_relaxed);
        write_idx_.store(next(w), std::memory_order_release);
    }

    // 消费者：获取可读的槽位指针（如果队列非空）
    T* get_read_slot() {
        size_t r = read_idx_.load(std::memory_order_relaxed);
        size_t w = write_idx_.load(std::memory_order_acquire);
        if (r == w) return nullptr; // empty
        return &buffer_[r];
    }

    // 消费者：释放当前读槽位（必须紧跟在 get_read_slot 之后）
    void commit_read() {
        size_t r = read_idx_.load(std::memory_order_relaxed);
        read_idx_.store(next(r), std::memory_order_release);
    }

    // 辅助：检查状态（调试用）
    bool empty() const {
        return read_idx_.load(std::memory_order_acquire) == write_idx_.load(std::memory_order_acquire);
    }
    bool full() const {
        size_t w = write_idx_.load(std::memory_order_acquire);
        size_t r = read_idx_.load(std::memory_order_acquire);
        return next(w) == r;
    }

private:
    size_t next(size_t idx) const { return (idx + 1) % Capacity; }
    std::atomic<size_t> write_idx_;
    std::atomic<size_t> read_idx_;
    T buffer_[Capacity];
};

// 辅助函数：在共享内存中创建 RingBuf
template<typename T, size_t N>
ZeroCopyRingBuf<T, N>* create_shared_ringbuf() {
    size_t size = sizeof(ZeroCopyRingBuf<T, N>);
    void* addr = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (addr == MAP_FAILED) throw std::runtime_error("mmap failed");
    return new(addr) ZeroCopyRingBuf<T, N>();
}

// 辅助函数：销毁共享内存 RingBuf
template<typename T, size_t N>
void destroy_shared_ringbuf(ZeroCopyRingBuf<T, N>* ring) {
    if (ring) {
        ring->~ZeroCopyRingBuf<T, N>();
        munmap(ring, sizeof(ZeroCopyRingBuf<T, N>));
    }
}

#endif