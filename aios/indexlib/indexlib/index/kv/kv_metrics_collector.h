#ifndef __INDEXLIB_KV_METRICS_COLLECTOR_H
#define __INDEXLIB_KV_METRICS_COLLECTOR_H

#include <tr1/memory>
#include <autil/TimeUtility.h>
#include "indexlib/common_define.h"
#include "indexlib/file_system/read_option.h"

IE_NAMESPACE_BEGIN(index);

class KVMetricsCollector
{
public:
    KVMetricsCollector()
    {
        BeginQuery();
    }

public:
    // Read
    int64_t GetBlockCacheHitCount() const { return mBlockCounter.blockCacheHitCount; }
    int64_t GetBlockCacheMissCount() const { return mBlockCounter.blockCacheMissCount; }
    int64_t GetBlockCacheReadLatency() const { return mBlockCounter.blockCacheReadLatency; }
    int64_t GetSearchCacheHitCount() const { return mSearchCacheHitCount; }
    int64_t GetSearchCacheMissCount() const { return mSearchCacheMissCount; }
    int64_t GetPrepareLatency() const { return mPrepareLatency; }

    int64_t GetMemTableLatency() const { return mMemTableLatency; }
    int64_t GetSSTableLatency() const { return mSSTableLatency; }
    int64_t GetMemTableCount() const { return mMemTableCount; }
    int64_t GetSSTableCount() const { return mSSTableCount; }
    int64_t GetSearchCacheResultCount() const { return mSearchCacheResultCount; }

    // IO
    int64_t GetBlockCacheIOCount() const { return mBlockCounter.blockCacheIOCount; }
    int64_t GetBlockCacheIODataSize() const { return mBlockCounter.blockCacheIODataSize; }
    int64_t GetSKeyDataSizeInBlocks() const { return mSKeyDataSizeInBlocks; }
    int64_t GetValueDataSizeInBlocks() const { return mValueDataSizeInBlocks; }
    int64_t GetSKeyChunkCountInBlocks() const { return mSKeyChunkCountInBlocks; }
    int64_t GetValueChunkCountInBlocks() const { return mValueChunkCountInBlocks; }

    // legacy for compitable
    // LEGACY: Assume query order: Prepare --> Online(Building-->RT-Build) --> Offline(INC-Built)
    int64_t GetOnlineSegmentsLatency() const { return mMemTableLatency; }
    int64_t GetOfflineSegmentsLatency() const { return mSSTableLatency; }
    int64_t GetOnlineResultCount() const { return mMemTableCount; }
    int64_t GetOfflineResultCount() const { return mSSTableCount; }
    int64_t GetBlockCacheReadCount() const { return mBlockCounter.blockCacheMissCount; }

public:
    // Write
    void Reset()
    {
        mSearchCacheHitCount = 0;
        mSearchCacheMissCount = 0;
        mPrepareLatency = 0;
        mMemTableLatency = 0;
        mSSTableLatency = 0;
        mMemTableCount = 0;
        mSSTableCount = 0;
        mSearchCacheResultCount = 0;
        mSKeyDataSizeInBlocks = 0;
        mValueDataSizeInBlocks = 0;
        mSKeyChunkCountInBlocks = 0;
        mValueChunkCountInBlocks = 0;
        ResetBlockCounter();
        BeginQuery();
    }
    file_system::BlockAccessCounter* GetBlockCounter() { return &mBlockCounter; }
    void ResetBlockCounter() { mBlockCounter.Reset(); }
    void IncSearchCacheHitCount(int64_t count) { mSearchCacheHitCount += count; }
    void IncSearchCacheMissCount(int64_t count) { mSearchCacheMissCount += count; }

    void IncSKeyDataSizeInBlocks(int64_t size) { mSKeyDataSizeInBlocks += size; }
    void IncValueDataSizeInBlocks(int64_t size) { mValueDataSizeInBlocks += size; }
    void IncSKeyChunkCountInBlocks(int64_t count) { mSKeyChunkCountInBlocks += count; }
    void IncValueChunkCountInBlocks(int64_t count) { mValueChunkCountInBlocks += count; }

    // Assume query order: Prepare --> MemTable(Building) --> SSTable(not-incache-on-disk, cache, on-disk)
    void BeginQuery()
    {
        mBegin = autil::TimeUtility::currentTimeInMicroSeconds();
        mState = PREPARE;
    }
    void BeginMemTableQuery()
    {
        assert(mState == PREPARE);
        assert(mBegin != 0);
        int64_t end = autil::TimeUtility::currentTimeInMicroSeconds();
        mPrepareLatency += (end - mBegin);
        mBegin = end;
        mState = MEMTABLE;
    }
    void BeginSSTableQuery()
    {
        assert(mState == MEMTABLE || mState == SSTABLE);
        if (mState != SSTABLE)
        {
            assert(mBegin != 0);
            int64_t end = autil::TimeUtility::currentTimeInMicroSeconds();
            mMemTableLatency += (end - mBegin);
            mBegin = end;
            mState = SSTABLE;
        }
    }
    void EndQuery()
    {
        int64_t end = autil::TimeUtility::currentTimeInMicroSeconds();
        if (mBegin == 0)
        {
            mState = PREPARE;
            mBegin = end;
            return;
        }
        if (mState == SSTABLE)
        {
            mSSTableLatency += (end - mBegin);
        }
        else if (mState == MEMTABLE)
        {
            mMemTableLatency += (end - mBegin);
        }
        else
        {
            assert(mState == PREPARE);
            mPrepareLatency += (end - mBegin);
        }
        mState = PREPARE;
        mBegin = end;
    }

    void IncSearchCacheResultCount(int64_t count)
    {
        mSearchCacheResultCount += count;
    }

    void IncResultCount()
    {
        if (mState == SSTABLE)
        {
            ++mSSTableCount;
        }
        else
        {
            assert(mState == MEMTABLE);
            ++mMemTableCount;
        }
    }
    void BeginStage()
    {
        mBegin = autil::TimeUtility::currentTimeInMicroSeconds();
    }
    void EndStage()
    {
        int64_t end = autil::TimeUtility::currentTimeInMicroSeconds();
        if (mState == SSTABLE)
        {
            mSSTableLatency += (end - mBegin);
        }
        else if (mState == MEMTABLE)
        {
            mMemTableLatency += (end - mBegin);
        }
        else
        {
            assert(mState == PREPARE);
            mPrepareLatency += (end - mBegin);
        }
        mBegin = 0;
    }
    
private:
    // search cache
    int64_t mSearchCacheHitCount = 0;
    int64_t mSearchCacheMissCount = 0;
    // query
    int64_t mPrepareLatency = 0;     // us
    int64_t mMemTableLatency = 0;    // us
    int64_t mSSTableLatency = 0;     // us
    int64_t mMemTableCount = 0;
    int64_t mSSTableCount = 0;       // including cache
    int64_t mSearchCacheResultCount = 0;

    // block cache IO
    int64_t mSKeyDataSizeInBlocks = 0;
    int64_t mValueDataSizeInBlocks = 0;
    int64_t mSKeyChunkCountInBlocks = 0;
    int64_t mValueChunkCountInBlocks = 0;

    file_system::BlockAccessCounter mBlockCounter;

private:
    enum State
    {
        PREPARE = 1,
        MEMTABLE  = 2,
        SSTABLE = 3,
    };

private:
    int64_t mBegin = 0;
    State mState = PREPARE;

};

DEFINE_SHARED_PTR(KVMetricsCollector);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_KV_METRICS_COLLECTOR_H
