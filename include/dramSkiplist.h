#include "skiplist.h"
#include "checkpoint.h"
#include "dramInodePool.h"
#include "pmemVnodePool.h"
#include "ckpt_log.h"
#include "common.h"
#include "valuelist.h"
#include <map>
#include <shared_mutex>
#include <unordered_map>
#pragma once

#define TLS_PIVOT_MAX 2

class CacheShard {
public:
    std::map<Key_t, Inode*> table;
    mutable std::shared_mutex mtx;
};

class ParentShard {
public:
    std::unordered_map<Inode*, Inode*> map;
    mutable std::shared_mutex mtx;
};

struct InodeSnapShort {
    int      last_index{-1};
    int      next{-1};
    int16_t  idx{-1};
    Key_t    gp_key{0};
    int      gp_value{-1};
    Key_t    sgp_key{0};
    int      sgp_value{-1};    int16_t  sgp_idx{-1};       // SGP position in inode's SGP array (-1 if no SGP)    
    Key_t    lb_key{0};
    Key_t    ub_key{std::numeric_limits<Key_t>::max()};

    uint64_t ver_snap{0};
};

struct CachedVnodeImage {
    uint64_t bitmap;
    vnode_entry records[vnode_fanout];
};

struct TlsVnodeCopyEntry {
    int           vnode_id{-1};
    BloomFilter*  bf{nullptr};
    uint64_t      bf_ver{0};
    Key_t         lb{0};
    Key_t         ub{std::numeric_limits<Key_t>::max()};
    CachedVnodeImage* img{nullptr};
    uint64_t      last_use{0};
};

struct TlsVnodeCopyCache {
    static constexpr size_t kCap = 8;

    alignas(64) CachedVnodeImage images[kCap];

    TlsVnodeCopyEntry slots[kCap];
    size_t hand{0};

    TlsVnodeCopyCache() {
        for (size_t i = 0; i < kCap; ++i) {
            slots[i].vnode_id = -1;
            slots[i].bf       = nullptr;
            slots[i].bf_ver   = 0;
            slots[i].lb       = 0;
            slots[i].ub       = std::numeric_limits<Key_t>::max();
            slots[i].img      = &images[i];
            slots[i].last_use = 0;
            images[i].bitmap  = 0;
        }
    }

    void reset_slot(size_t i) {
        slots[i].vnode_id = -1;
        slots[i].bf       = nullptr;
        slots[i].bf_ver   = 0;
        slots[i].lb       = 0;
        slots[i].ub       = std::numeric_limits<Key_t>::max();
        slots[i].last_use = 0;
        images[i].bitmap  = 0;
    }
};

struct TLSShadowEntry {
    int        vnode_id{-1};
    uint64_t   version{0};     // BloomFilter::version
    uint64_t   bitmap{0};
    uint64_t   last_use{0};
    vnode_entry entries[vnode_fanout];
};

struct TLSShadowCache {
    static constexpr size_t kCap = 32;
    TLSShadowEntry slots[kCap];
    uint64_t clock{1};

    TLSShadowEntry* find(int vid) {
        for (auto &e : slots) if (e.vnode_id == vid) return &e;
        return nullptr;
    }
    TLSShadowEntry* victim() {
        size_t idx = 0; uint64_t oldest = std::numeric_limits<uint64_t>::max();
        for (size_t i = 0; i < kCap; ++i) {
            if (slots[i].vnode_id < 0) return &slots[i];
            if (slots[i].last_use < oldest) { oldest = slots[i].last_use; idx = i; }
        }
        return &slots[idx];
    }
};

class DramSkiplist {
private:
#ifdef ENABLE_L1_TLS_CACHE
    std::atomic<uint32_t> global_epoch{0};

    struct TlsPivot {
        Inode*   node{nullptr};
        Key_t    min_key{0};
        Key_t    upper_key{0};
        uint32_t epoch{0};
        uint8_t  fail_cnt{0};
        uint16_t hit_cnt{0};
    };
    static thread_local struct {
        TlsPivot pivots[TLS_PIVOT_MAX];
        int used;
    } tls_pivot_set_;

    Inode* tls_try_match(Key_t key, int& start_level);
    void   tls_record_pivot(Inode* node);
    void   tls_mark_fail(Key_t min_key);
    void   bump_epoch();
#endif

public:
    Inode* header[MAX_LEVEL];
    Inode* tail[MAX_LEVEL];
    DramInodePool *dramInodePool;
    CkptLog *ckpt_log;
    ValueList *valueList;
    std::atomic<int> level{0}; //level is the current max level of the skiplist
    // level_lock removed: replaced by atomic CAS on level
    vector<int> inode_count_on_each_level;

    std::map<Key_t, Inode*> lookup_cache;
    std::shared_mutex cache_mutex;

    static constexpr size_t kNumShards = 64;

    std::array<CacheShard, kNumShards> cache_shards;
    std::hash<Key_t> key_hasher;

    inline size_t shard_of(const Key_t key) {
        return key_hasher(key) % kNumShards;
    }
    Inode* find_start_node_from_cache_shards(Key_t key, int& start_level);
    void populate_cache_shards(Key_t key, Inode* node, int current_total_level);

    std::vector<Inode*> nodesCoveringRangeAtLevel(uint64_t a, uint64_t b, int level);
    Inode* nextAtLevel(Inode* n) const;


private:
    static thread_local Key_t  tls_pivot_key_;
    static thread_local Inode* tls_pivot_node_;
    static thread_local TlsVnodeCopyCache tls_vnode_copy_cache_;
    static thread_local uint64_t          tls_vnode_copy_lru_clock_;
    void invalidate_tls_pivot();
    void update_tls_pivot(Key_t key, Inode* node);

    Key_t get_node_upper_bound(Inode* node);

public:
    std::shared_mutex inode_locks[MAX_NODES];
    std::shared_mutex rebalance_lock;
    DramSkiplist(CkptLog *ckp_log, DramInodePool *dramInodePool, ValueList *valuelist);
    ~DramSkiplist();
    bool update(Key_t &oldKey, Key_t &newKey, Val_t &val);
    bool add(Vnode *targetVnode);
    // return the index in gps of the index node that poionts to the vnode
    //Inode *lookup(Key_t key, Inode *current, int currentHighestLevelIndex, std::shared_lock<std::shared_mutex> &current_lock, int &idx);
    Inode *lookup(Key_t key, Inode *current, int currentHighestLevelIndex, int &idx, InodeSnapShort &snap);
    Inode* lookupForInsert(Key_t key, Inode* &start, int level, int& idx, std::vector<Inode*>& updates);
    Inode* lookupForInsertWithSnap(Key_t key, Inode* &start,
                                   int currentHighestLevelIndex,
                                   int &idx, bool &is_sgp,
                                   std::vector<Inode*> &updates,
                                   InodeSnapShort &snap);

    bool validateSnapShort(Inode* n, const InodeSnapShort& s, Key_t key) const;

    Inode *getHeader();
    Inode *getHeader(int level);
    void getPivotNodesForInsert(Key_t key, Inode* updates[]);
    bool rebalanceInode(Inode *inode, bool lastLevel);
    int generateRandomLevel();
    bool rebalanceIndex(Vnode &targetVnode);
    int rebalanceIdx(Vnode &targetVnode, Key_t targetKey);
    bool activateGP(Inode &inode);
    void setLevel(int level);
    int getLevel();

    static constexpr size_t kNumParentShards = 64;
    static_assert((kNumParentShards & (kNumParentShards - 1)) == 0, "kNumParentShards must be power of two");
    ParentShard parent_shards[kNumParentShards];

    inline size_t parent_shard_of(const Inode* child) const {
        uintptr_t x = reinterpret_cast<uintptr_t>(child);
        return (x >> 6) & (kNumParentShards - 1);
    }

    Inode* getParentInode(Inode* &child);
    //void removeInodeRelation(Inode* &child);
    void acquireLocksInOrder(std::vector<Inode*>& nodes, std::vector<std::unique_lock<std::shared_mutex>>& locks);
    void acquireWriteLocksInOrderByVersion(std::vector<Inode*>& nodes);
    void releaseWriteLocksInOrderByVersion(std::vector<Inode*>& nodes);
    int fastRebalance(Inode* &inode, Inode* &parent_inode);
    dram_log_entry_t *create_log_entry(Inode *inode);
    bool isTail(uint32_t id) {
        return (id >= MAX_LEVEL && id < 2 * MAX_LEVEL);
    }

    bool increaseLevel()
    {
        int cur = level.load(std::memory_order_acquire);
        while (cur < MAX_LEVEL - 1) {
            if (level.compare_exchange_weak(cur, cur + 1,
                    std::memory_order_acq_rel, std::memory_order_acquire))
                return true;
        }
        return false;
    }
    void printStats();
    double calculateSearchEfficiency(long count);
    void fillInodeCountEachLevel(int level);

    bool find_and_verify_candidate_parent(Inode* inode, Inode* parent_hint, 
                              Inode*& candidate_parent, Inode*& candidate_next, 
                              Inode*& header_above);

#if ENABLE_DELTA_LOG
    void ckpt_log_single_slot_delta(CkptLog *log, Inode *inode, int16_t slot);
    void ckpt_log_multi_slots_delta(CkptLog *log, Inode *inode, const std::vector<int16_t> &slots);
#endif

    bool validateSnapShort(Inode* n, const InodeSnapShort& s) const;
};
