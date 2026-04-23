#include "pmemValuePool.h"

#if ENABLE_VARIABLE_PAYLOAD

#include <iostream>

PmemValuePool::PmemValuePool(const std::string &storagePath, size_t poolBytes) {
    // Runtime pool-size override: TANDEMKV_VALPOOL_BYTES=<bytes>
    // Example: TANDEMKV_VALPOOL_BYTES=$((120*1024*1024*1024)) for 120 GB
    if (const char *env = std::getenv("TANDEMKV_VALPOOL_BYTES")) {
        size_t v = static_cast<size_t>(std::strtoull(env, nullptr, 10));
        if (v > 0) poolBytes = v;
    }
    fileName_ = storagePath + "/pmemValuePool";

    // libpmemobj overhead headroom (matches PmemVnodePool::init pattern).
    const size_t pool_headroom = 512ULL * 1024 * 1024;
    const size_t total_pool_size = poolBytes + pool_headroom;

    root_obj *root = nullptr;
    bool isCreate = false;
    bool ret = PmemManager::createOrOpenPool(PMEM_VARPAYLOAD_POOL_ID,
                                             fileName_.c_str(),
                                             total_pool_size,
                                             (void **)&root,
                                             isCreate);
    if (!ret) {
        std::cerr << "PmemValuePool: failed to create/open pool: " << fileName_ << std::endl;
        std::exit(1);
    }

    PMEMobjpool *pop = (PMEMobjpool *)PmemManager::getPoolStartAddress(PMEM_VARPAYLOAD_POOL_ID);
    if (isCreate) {
        int rc = pmemobj_alloc(pop, &root->ptr[0], poolBytes, 0, NULL, NULL);
        if (rc) {
            std::cerr << "PmemValuePool: failed to alloc value region (" << poolBytes << " bytes)" << std::endl;
            std::exit(1);
        }
    }

    base_ = static_cast<char *>(pmemobj_direct(root->ptr[0]));
    capacity_ = poolBytes;
    currentOffset_.store(kFirstOffset, std::memory_order_relaxed);
}

uint64_t PmemValuePool::append(const void *data, uint32_t len) {
    const size_t record_bytes = roundUp8(sizeof(VarPayloadHeader) + len);
    const uint64_t handle = currentOffset_.fetch_add(record_bytes, std::memory_order_relaxed);
    if (handle + record_bytes > capacity_) {
        currentOffset_.fetch_sub(record_bytes, std::memory_order_relaxed);
        std::fprintf(stderr,
            "\nPmemValuePool: pool exhausted (capacity=%zu B, offset=%zu B, "
            "record=%zu B).\n"
            "  Increase with: TANDEMKV_VALPOOL_BYTES=<bytes>\n"
            "  e.g. TANDEMKV_VALPOOL_BYTES=%zu  (2x current)\n",
            capacity_, static_cast<size_t>(handle),
            record_bytes, capacity_ * 2);
        std::exit(1);
    }

    char *rec = base_ + handle;
    VarPayloadHeader hdr{ len, 0 };
    std::memcpy(rec, &hdr, sizeof(hdr));
    if (len > 0) {
        std::memcpy(rec + sizeof(hdr), data, len);
    }

    // Persist the new record. Recovery is deferred, but durability of the
    // bytes themselves is cheap and keeps the option open later.
    PmemManager::flushNoDrain(PMEM_VARPAYLOAD_POOL_ID, rec, record_bytes);
    PmemManager::drain(PMEM_VARPAYLOAD_POOL_ID);

    return handle;
}

bool PmemValuePool::updateInPlace(uint64_t handle, const void *data, uint32_t new_len)
{
    if (handle < kFirstOffset || handle + sizeof(VarPayloadHeader) > capacity_) {
        return false;
    }
    char *rec = base_ + handle;
    VarPayloadHeader hdr;
    std::memcpy(&hdr, rec, sizeof(hdr));

    // Slot size is fixed at allocation time; new payload must fit.
    const size_t slot_bytes = roundUp8(sizeof(VarPayloadHeader) + hdr.len);
    if (sizeof(VarPayloadHeader) + new_len > slot_bytes) {
        return false;
    }

    if (new_len != hdr.len) {
        hdr.len = new_len;
        std::memcpy(rec, &hdr, sizeof(hdr));
    }
    if (new_len > 0) {
        std::memcpy(rec + sizeof(hdr), data, new_len);
    }
    PmemManager::flushNoDrain(PMEM_VARPAYLOAD_POOL_ID, rec, slot_bytes);
    PmemManager::drain(PMEM_VARPAYLOAD_POOL_ID);
    return true;
}

const void *PmemValuePool::read(uint64_t handle, uint32_t *outLen) const {
    if (handle < kFirstOffset || handle + sizeof(VarPayloadHeader) > capacity_) {
        if (outLen) *outLen = 0;
        return nullptr;
    }
    const char *rec = base_ + handle;
    VarPayloadHeader hdr;
    std::memcpy(&hdr, rec, sizeof(hdr));
    if (handle + sizeof(hdr) + hdr.len > capacity_) {
        if (outLen) *outLen = 0;
        return nullptr;
    }
    if (outLen) *outLen = hdr.len;
    return rec + sizeof(hdr);
}

#endif  // ENABLE_VARIABLE_PAYLOAD
