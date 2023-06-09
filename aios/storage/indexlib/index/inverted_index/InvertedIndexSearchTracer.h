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
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/util/cache/BlockAccessCounter.h"

namespace indexlib::util {
struct BlockAccessCounter;
}

namespace indexlib::index {

class InvertedIndexSearchTracer
{
public:
    void SetBitmapTerm() { _bitmapTerm = true; }
    void SetTruncateTerm() { _truncateTerm = true; }
    void SetSearchedSegmentCount(uint32_t count) { _searchedSegmentCount = count; }
    void SetSearchedInMemSegmentCount(uint32_t count) { _searchedInMemSegmentCount = count; }
    void IncDictionaryLookupCount(size_t count = 1) { _totalDictLookupCount += count; }
    void IncDictionaryHitCount(size_t count = 1) { _totalDictHitCount += count; }
    // void RecordLookupIo(size_t ioSize);

    bool IsBitmapTerm() const { return _bitmapTerm; }
    bool IsTruncateTerm() const { return _truncateTerm; }
    uint32_t GetSearchedSegmentCount() const { return _searchedSegmentCount; }
    uint32_t GetSearchedInMemSegmentCount() const { return _searchedInMemSegmentCount; }
    size_t GetDictionaryLookupCount() const { return _totalDictLookupCount; }
    size_t GetDictionaryHitCount() const { return _totalDictHitCount; }

    util::BlockAccessCounter* GetDictionaryBlockCacheCounter() { return &_dictionaryCounter; }
    util::BlockAccessCounter* GetPostingBlockCacheCounter() { return &_postingCounter; }

    const util::BlockAccessCounter* GetDictionaryBlockCacheCounter() const { return &_dictionaryCounter; }
    const util::BlockAccessCounter* GetPostingBlockCacheCounter() const { return &_postingCounter; }

    InvertedIndexSearchTracer& operator+=(const InvertedIndexSearchTracer& other) noexcept
    {
        _dictionaryCounter += *other.GetDictionaryBlockCacheCounter();
        _postingCounter += *other.GetPostingBlockCacheCounter();
        _searchedSegmentCount += other.GetSearchedSegmentCount();
        _searchedInMemSegmentCount += other.GetSearchedInMemSegmentCount();
        _totalDictLookupCount += other.GetDictionaryLookupCount();
        _totalDictHitCount += other.GetDictionaryHitCount();
        _bitmapTerm |= other._bitmapTerm;
        _truncateTerm |= other._truncateTerm;
        return *this;
    }

    friend std::ostream& operator<<(std::ostream& os, const InvertedIndexSearchTracer& tracer)
    {
        return os << "{dictCounter:{" << tracer._dictionaryCounter << "}, postingCounter:{" << tracer._postingCounter
                  << "}, searchedSegCnt:" << tracer.GetSearchedSegmentCount()
                  << ", searchedInMemSegCnt:" << tracer.GetSearchedInMemSegmentCount()
                  << ", totalDictLookupCnt:" << tracer.GetDictionaryLookupCount()
                  << ", totalDictHitCnt:" << tracer.GetDictionaryHitCount() << "}";
    }

protected:
    util::BlockAccessCounter _dictionaryCounter;
    util::BlockAccessCounter _postingCounter;

    uint32_t _searchedSegmentCount = 0;
    uint32_t _searchedInMemSegmentCount = 0;
    uint32_t _totalDictLookupCount = 0;
    uint32_t _totalDictHitCount = 0;

    bool _bitmapTerm = false;
    bool _truncateTerm;
};

} // namespace indexlib::index
