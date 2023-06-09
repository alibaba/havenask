/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <cassert>
#include <memory>

#include "autil/TimeUtility.h"
#include "indexlib/util/cache/BlockAccessCounter.h"
#include "indexlib/util/cache/SearchCacheCounter.h"

namespace indexlibv2::index {

class KVMetricsCollector
{
public:
    KVMetricsCollector() { BeginQuery(); }

public:
    // Read
    int64_t GetBlockCacheHitCount() const { return _blockCounter.blockCacheHitCount; }
    int64_t GetBlockCacheMissCount() const { return _blockCounter.blockCacheMissCount; }
    int64_t GetBlockCacheReadLatency() const { return _blockCounter.blockCacheReadLatency; }
    int64_t GetSearchCacheHitCount() const { return _searchCacheCounter.hitCount; }
    int64_t GetSearchCacheMissCount() const { return _searchCacheCounter.missCount; }
    int64_t GetPrepareLatency() const { return _prepareLatency; }

    int64_t GetMemTableLatency() const { return _memTableLatency; }
    int64_t GetSSTableLatency() const { return _sstTableLatency; }
    // TODO(xinfei.sxf) rename these function name (add doc count suffix, use building / built)
    int64_t GetMemTableCount() const { return _memTableCount; }
    int64_t GetSSTableCount() const { return _sstTableCount; }
    int64_t GetSearchCacheResultCount() const { return _searchCacheResultCount; }

    // IO
    int64_t GetBlockCacheIOCount() const { return _blockCounter.blockCacheIOCount; }
    int64_t GetBlockCacheIODataSize() const { return _blockCounter.blockCacheIODataSize; }
    int64_t GetSKeyDataSizeInBlocks() const { return _skeyDataSizeInBlocks; }
    int64_t GetValueDataSizeInBlocks() const { return _valueDataSizeInBlocks; }
    int64_t GetSKeyChunkCountInBlocks() const { return _skeyChunkCountInBlocks; }
    int64_t GetValueChunkCountInBlocks() const { return _valueChunkCountInBlocks; }

public:
    // Write
    void Reset()
    {
        _prepareLatency = 0;
        _memTableLatency = 0;
        _sstTableLatency = 0;
        _memTableCount = 0;
        _sstTableCount = 0;
        _searchCacheResultCount = 0;
        _skeyDataSizeInBlocks = 0;
        _valueDataSizeInBlocks = 0;
        _skeyChunkCountInBlocks = 0;
        _valueChunkCountInBlocks = 0;
        ResetBlockCounter();
        ResetSearchCounter();
        BeginQuery();
    }
    indexlib::util::BlockAccessCounter* GetBlockCounter() { return &_blockCounter; }
    indexlib::util::SearchCacheCounter* GetSearchCacheCounter() { return &_searchCacheCounter; }

    void ResetBlockCounter() { _blockCounter.Reset(); }
    void ResetSearchCounter() { _searchCacheCounter.Reset(); }

    void IncSKeyDataSizeInBlocks(int64_t size) { _skeyDataSizeInBlocks += size; }
    void IncValueDataSizeInBlocks(int64_t size) { _valueDataSizeInBlocks += size; }
    void IncSKeyChunkCountInBlocks(int64_t count) { _skeyChunkCountInBlocks += count; }
    void IncValueChunkCountInBlocks(int64_t count) { _valueChunkCountInBlocks += count; }

    // Assume query order: Prepare --> MemTable(Building) --> SSTable(not-incache-on-disk, cache, on-disk)
    void BeginQuery()
    {
        _begin = autil::TimeUtility::currentTimeInMicroSeconds();
        _state = PREPARE;
    }
    void BeginMemTableQuery()
    {
        assert(_state == PREPARE);
        assert(_begin != 0);
        int64_t end = autil::TimeUtility::currentTimeInMicroSeconds();
        _prepareLatency += (end - _begin);
        _begin = end;
        _state = MEMTABLE;
    }
    void BeginSSTableQuery()
    {
        assert(_state == MEMTABLE || _state == SSTABLE);
        if (_state != SSTABLE) {
            assert(_begin != 0);
            int64_t end = autil::TimeUtility::currentTimeInMicroSeconds();
            _memTableLatency += (end - _begin);
            _begin = end;
            _state = SSTABLE;
        }
    }
    void EndQuery()
    {
        int64_t end = autil::TimeUtility::currentTimeInMicroSeconds();
        if (_begin == 0) {
            _state = PREPARE;
            _begin = end;
            return;
        }
        if (_state == SSTABLE) {
            _sstTableLatency += (end - _begin);
        } else if (_state == MEMTABLE) {
            _memTableLatency += (end - _begin);
        } else {
            assert(_state == PREPARE);
            if (0 == _prepareLatency) {
                _prepareLatency += (end - _begin);
            }
        }
        _state = PREPARE;
        _begin = end;
    }

    void IncSearchCacheResultCount(int64_t count) { _searchCacheResultCount += count; }

    void IncResultCount(int64_t count)
    {
        if (_state == SSTABLE) {
            _sstTableCount += count;
        } else {
            assert(_state == MEMTABLE);
            _memTableCount += count;
        }
    }

    void IncResultCount()
    {
        if (_state == SSTABLE) {
            ++_sstTableCount;
        } else {
            assert(_state == MEMTABLE);
            ++_memTableCount;
        }
    }

    void BeginStage() { _begin = autil::TimeUtility::currentTimeInMicroSeconds(); }
    void EndStage()
    {
        int64_t end = autil::TimeUtility::currentTimeInMicroSeconds();
        if (_state == SSTABLE) {
            _sstTableLatency += (end - _begin);
        } else if (_state == MEMTABLE) {
            _memTableLatency += (end - _begin);
        } else {
            assert(_state == PREPARE);
            _prepareLatency += (end - _begin);
        }
        _begin = 0;
    }

    KVMetricsCollector& operator+=(const KVMetricsCollector& other)
    {
        _prepareLatency += other._prepareLatency;
        _memTableLatency += other._memTableLatency;
        _sstTableLatency += other._sstTableLatency;
        _memTableCount += other._memTableCount;
        _sstTableCount += other._sstTableCount;
        _searchCacheResultCount += other._searchCacheResultCount;

        _skeyDataSizeInBlocks += other._skeyDataSizeInBlocks;
        _valueChunkCountInBlocks += other._valueChunkCountInBlocks;
        _skeyChunkCountInBlocks += other._skeyChunkCountInBlocks;
        _valueChunkCountInBlocks += other._valueChunkCountInBlocks;

        _searchCacheCounter += other._searchCacheCounter;
        _blockCounter += other._blockCounter;
        return *this;
    }

    void setSSTableLatency(int64_t tableLatency) { _sstTableLatency = tableLatency; }

private:
    // query
    int64_t _prepareLatency = 0;  // us
    int64_t _memTableLatency = 0; // us
    int64_t _sstTableLatency = 0;  // us
    int64_t _memTableCount = 0;
    int64_t _sstTableCount = 0; // including cache
    int64_t _searchCacheResultCount = 0;

    // block cache IO
    int64_t _skeyDataSizeInBlocks = 0;
    int64_t _valueDataSizeInBlocks = 0;
    int64_t _skeyChunkCountInBlocks = 0;
    int64_t _valueChunkCountInBlocks = 0;

    indexlib::util::SearchCacheCounter _searchCacheCounter;
    indexlib::util::BlockAccessCounter _blockCounter;

private:
    enum State {
        PREPARE = 1,
        MEMTABLE = 2,
        SSTABLE = 3,
    };

private:
    int64_t _begin = 0;
    State _state = PREPARE;
};

} // namespace indexlibv2::index
