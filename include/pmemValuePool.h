#pragma once
#include <atomic>
#include <cstdint>
#include <cstring>
#include <string>
#include <libpmemobj.h>
#include "pmemManager.h"

#ifndef ENABLE_VARIABLE_PAYLOAD
#define ENABLE_VARIABLE_PAYLOAD 0
#endif

#if ENABLE_VARIABLE_PAYLOAD

// Slot in PmemManager::pmemPool[6]; 0/1/3/4 are taken (vnode/inode/ckpt/bf).
#define PMEM_VARPAYLOAD_POOL_ID 5

// Per-record header. Total record size = sizeof(VarPayloadHeader) + payload bytes,
// rounded up to 8 to keep both the payload and the next record's header 8B-aligned.
struct VarPayloadHeader {
    uint32_t len;       // payload length in bytes
    uint32_t reserved;  // padding so the payload starts 8B-aligned
};
static_assert(sizeof(VarPayloadHeader) == 8, "VarPayloadHeader must be 8 bytes");

class PmemValuePool {
public:
    // Default 10 GB pool, matching min_pool_size pattern in PmemVnodePool.
    static constexpr size_t kDefaultPoolBytes = 10ULL * 1024 * 1024 * 1024;
    // Bump-pointer alignment. Records are appended at multiples of this so
    // every header is 8B-aligned (and AVX/NT-store paths stay happy if used).
    static constexpr size_t kAlign = 8;
    // Reserve offset 0 as a "null handle" sentinel; first valid handle is 8.
    static constexpr uint64_t kFirstOffset = 8;

    PmemValuePool(const std::string &storagePath, size_t poolBytes = kDefaultPoolBytes);
    ~PmemValuePool() = default;

    // Append a record; returns its handle (byte offset in the pool region).
    // Aborts the process on out-of-space (use TANDEMKV_VALPOOL_BYTES to size up).
    uint64_t append(const void *data, uint32_t len);

    // Overwrite an existing record's payload in-place. Succeeds only if new_len
    // fits within the original slot. Returns false if handle is invalid or the
    // new payload is too large for the slot (caller should fall back to append).
    bool updateInPlace(uint64_t handle, const void *data, uint32_t new_len);

    // Read a record; returns pointer to payload bytes and writes length to *outLen.
    // Returns nullptr if handle is 0 or out of range.
    const void *read(uint64_t handle, uint32_t *outLen) const;

    uint64_t getCurrentOffset() const { return currentOffset_.load(std::memory_order_relaxed); }
    size_t getCapacity() const { return capacity_; }

private:
    static inline size_t roundUp8(size_t n) {
        return (n + (kAlign - 1)) & ~static_cast<size_t>(kAlign - 1);
    }

    std::string fileName_;
    char *base_ = nullptr;          // start of the value-region inside the PMEM pool
    size_t capacity_ = 0;           // bytes available in the value region
    std::atomic<uint64_t> currentOffset_{kFirstOffset};
};

#endif  // ENABLE_VARIABLE_PAYLOAD
