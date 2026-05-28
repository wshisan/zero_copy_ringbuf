#include <iostream>
#include <thread>
#include <chrono>
#include <cassert>
#include "zero_copy_ringbuf.h"

// 定义一个较大的数据结构，模拟障碍物列表或图像元数据
struct BigData {
    double values[64];      // 512 字节
    uint64_t timestamp;
    int id;
};

constexpr size_t QUEUE_CAPACITY = 32;

void producer(ZeroCopyRingBuf<BigData, QUEUE_CAPACITY>* ring) {
    int id = 0;
    while (true) {
        // 1. 获取可写的槽位指针（零拷贝）
        BigData* slot = ring->get_write_slot();
        if (slot == nullptr) {
            // 队列满，稍等（或者可用条件变量，这里简单让出 CPU）
            std::this_thread::yield();
            continue;
        }

        // 2. 直接在共享内存上构造数据（避免任何拷贝）
        slot->id = id;
        slot->timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch()
        ).count();
        for (int i = 0; i < 64; ++i) {
            slot->values[i] = id * 0.1 + i;
        }

        // 3. 提交，使消费者可见
        ring->commit_write();

        if (id % 100 == 0) {
            std::cout << "Producer: produced id=" << id << std::endl;
        }
        ++id;
        if (id >= 1000) break; // 生产 1000 个后退出

        // 模拟生产间隔
        std::this_thread::sleep_for(std::chrono::microseconds(500));
    }
}

void consumer(ZeroCopyRingBuf<BigData, QUEUE_CAPACITY>* ring) {
    int last_id = -1;
    while (true) {
        // 1. 获取可读的槽位指针
        BigData* slot = ring->get_read_slot();
        if (slot == nullptr) {
            // 队列空，稍等
            std::this_thread::yield();
            continue;
        }

        // 2. 直接读取共享内存中的数据（零拷贝访问）
        int id = slot->id;
        uint64_t ts = slot->timestamp;
        double sum = 0;
        for (int i = 0; i < 64; ++i) sum += slot->values[i];

        // 3. 释放该槽位，允许生产者覆盖
        ring->commit_read();

        // 简单校验：id 应该递增
        if (id != last_id + 1) {
            std::cerr << "Consumer: missing data! expected " << last_id+1 << " got " << id << std::endl;
        }
        last_id = id;

        if (id % 100 == 0) {
            std::cout << "Consumer: consumed id=" << id << " sum=" << sum << std::endl;
        }

        if (id == 999) break; // 消费完 1000 个（id 0-999）
    }
}

int main() {
    std::cout << "创建共享内存环形队列（零拷贝）..." << std::endl;
    auto* ring = create_shared_ringbuf<BigData, QUEUE_CAPACITY>();

    std::cout << "启动生产者线程和消费者线程..." << std::endl;
    std::thread prod(producer, ring);
    std::thread cons(consumer, ring);

    prod.join();
    cons.join();

    destroy_shared_ringbuf(ring);
    std::cout << "测试完成，无额外数据拷贝。" << std::endl;
    return 0;
}