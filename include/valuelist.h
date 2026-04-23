#include <iostream>
#include <cstring>
#include <array>
#include <atomic>
#include <mutex>
#include "node.h"
#include "pmemVnodePool.h"
#include "pmemBFPool.h"
#include "pmemValuePool.h"
#include "common.h"


#pragma once
// Value list class on pmem

#ifndef ENABLE_DRAM_BLOOM_FULL
#define ENABLE_DRAM_BLOOM_FULL 0
#endif

#ifndef ENABLE_DRAM_BLOOM_HOT
#define ENABLE_DRAM_BLOOM_HOT 1
#endif

// Sanity: at most one DRAM bloom mode can be active
#if ENABLE_DRAM_BLOOM_FULL && ENABLE_DRAM_BLOOM_HOT
#error "Cannot enable both ENABLE_DRAM_BLOOM_FULL and ENABLE_DRAM_BLOOM_HOT"
#endif

#ifndef DRAM_BLOOM_CHUNK_SIZE
#define DRAM_BLOOM_CHUNK_SIZE 1048576UL
#endif

#if ENABLE_DRAM_BLOOM_HOT
#include "hotBloomCache.h"
#endif

class ValueList {
public:
    string fileName;
    PmemVnodePool *pmemVnodePool;
    PmemBFPool *pmemBFPool;
#if ENABLE_VARIABLE_PAYLOAD
    PmemValuePool *pmemValuePool = nullptr;
#endif
    Vnode *head;
#if ENABLE_DRAM_BLOOM_FULL
    static constexpr size_t kBloomChunkSize = DRAM_BLOOM_CHUNK_SIZE;
    static constexpr size_t kMaxBloomChunks =
        (MAX_VALUE_NODES + kBloomChunkSize - 1) / kBloomChunkSize;
    std::array<std::atomic<BloomFilter *>, kMaxBloomChunks> dramBloomChunks;
    std::mutex bloomAllocMutex;
#endif
#if ENABLE_DRAM_BLOOM_HOT
    HotBloomCache *hotBloomCache_ = nullptr;
#endif
public:
    ValueList(string storagePath, PmemBFPool *bfPool);
    ~ValueList();
    bool insert(Key_t key, Val_t value);
    bool insert(Vnode* startNode, Vnode* vnode);
    bool append(Vnode* curVnode, Vnode* nextVnode);
    bool split(Vnode* &curVnode, Vnode* &nextVnode);
    bool update(Key_t key, Val_t value);
    bool remove(Key_t key);
    bool recovery();
    Vnode *getNext(Vnode *curNode);
    size_t getAllocatedBloomCount() const;
    size_t getAllocatedBloomBytes() const;
    Vnode* getHeader()
    {
        return head;
    }
    int getKeyPos(Key_t key);

    void syncBloomToPMEM(size_t count);
    BloomFilter *getBloom(size_t vnodeId);
    BloomFilter *getBloomForWrite(size_t vnodeId);

#if ENABLE_VARIABLE_PAYLOAD
    // Variable-payload helpers. Handle 0 is reserved as the null sentinel.
    inline Val_t allocPayload(const void *data, uint32_t len) {
        return static_cast<Val_t>(pmemValuePool->append(data, len));
    }
    inline const void *readPayload(Val_t handle, uint32_t *outLen) const {
        return pmemValuePool->read(static_cast<uint64_t>(handle), outLen);
    }
#endif

#if ENABLE_DRAM_BLOOM_FULL
private:
    void ensureBloomForCount(size_t count);
#endif

#if ENABLE_DRAM_BLOOM_HOT
public:
    /// Access the hot bloom cache (for promoting blooms from external callers).
    HotBloomCache* hotBloomCache() { return hotBloomCache_; }
#endif
};
