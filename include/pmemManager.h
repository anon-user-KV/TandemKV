#include <libpmemobj.h>
#include <string>
#include <iostream>
#include <unistd.h>
#include <immintrin.h>   // AVX2 NT-Store intrinsics
#include <cstring>
#include "common.h"
#pragma once
using namespace std;

typedef struct root_obj {
    PMEMoid ptr[2];
    //    PMEMoid ptr2;
} root_obj;

class PmemManager {
public:
        static void *pmemPool[6]; // dram
    public:
        static void *getPoolStartAddress(int poolId) {
            return pmemPool[poolId];
        }

        static bool createOrOpenPool(int poolId, string path, size_t poolSize, void **rootp, bool &isCreate) {
            PMEMobjpool *pop = nullptr;
            if(access(path.c_str(), F_OK) != 0) {
                std::cout << "File does not exists: " << path << std::endl;
                pop = pmemobj_create(path.c_str(), "pmemvaluepool", poolSize, 0666);
                if (pop == nullptr) {
                    std::cout << "Failed to create pool: " << path << ", error code: " << errno << std::endl;
                    return false;
                }
                isCreate = true;
                std::cout << "Created pool: " << path << std::endl;                
            } else {
                std::cout << "File exist: " << path << std::endl;
                pop = pmemobj_open(path.c_str(), "pmemvaluepool");
                if(pop != NULL) {
                    std::cout << "Opened pool: " << path << std::endl;
                } else {
                    std::cout << "Failed to open pool: " << path << std::endl;
                    return false;
                }
                isCreate = false;
            }
            pmemPool[poolId] = reinterpret_cast<void *>(pop);
            PMEMoid root = pmemobj_root(pop, sizeof(root_obj));
            *rootp = (root_obj*)pmemobj_direct(root);
            return true;
        }

        static bool closePool(int poolId) {
            PMEMobjpool *pop = (PMEMobjpool *)pmemPool[poolId];
            pmemobj_close(pop);
            return true;
        }

        static void *alloc(int poolId, size_t size) {
            PMEMobjpool *pop = (PMEMobjpool *)pmemPool[poolId];
            PMEMoid oid;
            int ret = pmemobj_alloc(pop, &oid, size, 0, NULL, NULL);
            if (ret) {
                return nullptr;
            }
            return pmemobj_direct(oid);
        }

        static inline void flushToNVM(int poolId, char *data, size_t size) {
            PMEMobjpool *pop = (PMEMobjpool *)pmemPool[poolId];
            pmemobj_persist(pop, data, size);
        }

        static inline void flushNoDrain(int poolId, const void *addr, size_t len) {
            if (!addr || len == 0) return;
            PMEMobjpool *pop = (PMEMobjpool *)pmemPool[poolId];
            pmemobj_flush(pop, addr, len);
        }

        static inline void drain(int poolId) {
            PMEMobjpool *pop = (PMEMobjpool *)pmemPool[poolId];
            pmemobj_drain(pop);
        }

        static inline void memcpyToNVM(int poolId, char *dest, char *src, size_t size) {
            PMEMobjpool *pop = (PMEMobjpool *)pmemPool[poolId];
            pmemobj_memcpy_persist(pop, dest, src, size);
        }

        // static inline void memcpyNoDrain(...) { pmemobj_memcpy(pop, ...); pmemobj_flush(pop, dest, size); }
        static inline void memcpyToDRAM(int poolId, char *dest, char *src, size_t size) {
            [[maybe_unused]]PMEMobjpool *pop = (PMEMobjpool *)pmemPool[poolId];
            memcpy(dest, src, size);
        }

        // Non-temporal memcpy: bypass CPU cache, stream directly to memory controller WPQ.
        // Uses SSE2 _mm_stream_si128 (16-byte granularity) — matches vnode_entry size exactly.
        // Requires dest 16-byte aligned; does NOT issue sfence/drain.
        static inline void memcpyNTNoDrain(void *dest, const void *src, size_t len) {
            auto d = reinterpret_cast<char *>(dest);
            auto s = reinterpret_cast<const char *>(src);

            // Fast path: 16-byte aligned destination and length >= 16
            if (len >= 16 &&
                (reinterpret_cast<uintptr_t>(d) & 15) == 0) {
                size_t chunks = len / 16;
                auto *dst128 = reinterpret_cast<__m128i *>(d);
                auto *src128 = reinterpret_cast<const __m128i *>(s);
                for (size_t i = 0; i < chunks; ++i) {
                    __m128i v = _mm_loadu_si128(src128 + i);
                    _mm_stream_si128(dst128 + i, v);
                }
                size_t done = chunks * 16;
                // Tail bytes (< 16)
                if (done < len) {
                    std::memcpy(d + done, s + done, len - done);
                }
            } else {
                // Fallback: unaligned or tiny copy
                std::memcpy(dest, src, len);
            }
        }

        static inline unsigned char *align_ptr_to_cacheline(void *p)
        {
            return (unsigned char *)(((unsigned long)p + ~L1_CACHE_LINE_MASK) &
			L1_CACHE_LINE_MASK);
        }

        static inline unsigned long align_uint_to_cacheline(unsigned int size)
        {
            return (size + ~L1_CACHE_LINE_MASK) & L1_CACHE_LINE_MASK;
        }
};