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

#include <algorithm>
#include <assert.h>
#include <ext/alloc_traits.h>
#include <inttypes.h>
#include <memory>
#include <stddef.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "autil/Span.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Progress.h"

namespace indexlibv2::framework {

class Locator final
{
public:
    static_assert(sizeof(base::Progress) == 24, "Progress has been modified, deserialize may failed");
    static constexpr int32_t MAGIC_NUMBER = -502344248;
    static constexpr int32_t LOCATOR_DEFAULT_VERSION = 0;
    static constexpr int32_t LOCATOR_SUBOFFSET_VERSION = 1;

    enum class LocatorCompareResult {
        LCR_INVALID,        // source不合法
        LCR_SLOWER,         //比这个locator慢
        LCR_PARTIAL_FASTER, // 部分hash id慢
        LCR_FULLY_FASTER //完全比这个locator快(包括相等的场景，即这个locator拥有和他比较的locator的所有数据)
    };

    struct DocInfo {
        DocInfo() {}
        DocInfo(uint16_t hashId, int64_t timestamp, uint32_t concurrentIdx, uint8_t sourceIdx)
            : timestamp(timestamp)
            , concurrentIdx(concurrentIdx)
            , hashId(hashId)
            , sourceIdx(sourceIdx)
        {
        }
        // 表示doc的在数据源中的位置，与concurrent idx组成两级精确位置
        int64_t timestamp = indexlib::INVALID_TIMESTAMP;
        // 表示timestamp相同时的msg下标, timestamp相同时, concurrentIdx可能递增
        uint32_t concurrentIdx = 0;
        // 表示doc在数据源中的hash值，用于确定doc属于数据源的哪个范围
        uint16_t hashId = 0;
        // 同时读多个数据源时, 表示document来自于哪一个数据
        uint8_t sourceIdx = 0;

        base::Progress::Offset GetProgressOffset() const { return {timestamp, concurrentIdx}; }
        std::string DebugString() const;
    };

public:
    bool operator==(const Locator& other) const
    {
        if (_userData != other._userData) {
            return false;
        }
        return IsFasterThan(other, false) == LocatorCompareResult::LCR_FULLY_FASTER &&
               other.IsFasterThan(*this, false) == LocatorCompareResult::LCR_FULLY_FASTER;
    }
    bool operator!=(const Locator& other) const { return !(*this == other); }

    Locator() : _src(0), _minOffset(base::Progress::INVALID_OFFSET), _multiProgress({{}}) {}
    // TODO(tianxiao) remove structor
    Locator(uint64_t src, int64_t minOffset) : _src(src), _minOffset({minOffset, 0})
    {
        SetMultiProgress({{base::Progress(_minOffset)}});
    }
    Locator(uint64_t src, const base::Progress::Offset minOffset) : _src(src), _minOffset(minOffset)
    {
        SetMultiProgress({{base::Progress(_minOffset)}});
    }

    Locator(const Locator& other)
    {
        if (&other == this) {
            return;
        }
        this->_src = other._src;
        this->_minOffset = other._minOffset;
        this->_multiProgress = other._multiProgress;
        this->_userData = other._userData;
        this->_isLegacyLocator = other._isLegacyLocator;
    }
    Locator& operator=(const Locator& other)
    {
        if (&other == this) {
            return *this;
        }
        this->_src = other._src;
        this->_minOffset = other._minOffset;
        this->_multiProgress = other._multiProgress;
        this->_userData = other._userData;
        this->_isLegacyLocator = other._isLegacyLocator;
        return *this;
    }

    // other locator muster fully faster than current locator, or return false
    bool Update(const Locator& other);
    std::string Serialize() const;
    bool Deserialize(const std::string& str) { return Deserialize(str.data(), str.size()); }
    bool Deserialize(const autil::StringView& str) { return Deserialize(str.data(), str.size()); }
    void SetSrc(uint64_t src) { _src = src; }
    uint64_t GetSrc() const { return _src; }

    bool IsSameSrc(const Locator& otherLocator, bool ignoreLegacyDiffSrc) const;
    void SetOffset(const base::Progress::Offset& offset) { SetMultiProgress({{base::Progress(offset)}}); }
    base::Progress::Offset GetOffset() const { return _minOffset; }
    base::Progress::Offset GetMaxOffset() const
    {
        base::Progress::Offset ret = base::Progress::INVALID_OFFSET;
        for (const auto& progressVec : _multiProgress) {
            for (const auto& progress : progressVec) {
                ret = std::max(ret, progress.offset);
            }
        }
        return ret;
    }

    void SetMultiProgress(const base::MultiProgress& multiProgress)
    {
        if (multiProgress.empty() || multiProgress[0].empty()) {
            return;
        }
        _minOffset = multiProgress[0][0].offset;
        for (const auto& progressVec : multiProgress) {
            if (progressVec.empty()) {
                _minOffset = base::Progress::INVALID_OFFSET;
                return;
            }
            for (const auto& progress : progressVec) {
                _minOffset = std::min(_minOffset, progress.offset);
            }
        }
        _multiProgress = multiProgress;
    }
    const base::MultiProgress& GetMultiProgress() const { return _multiProgress; }
    void SetUserData(std::string userData) { _userData = std::move(userData); }
    const std::string& GetUserData() const { return _userData; }

    size_t Size() const;
    std::string DebugString() const;
    bool IsValid() const;
    Locator::LocatorCompareResult IsFasterThan(const DocInfo& docInfo) const;
    Locator::LocatorCompareResult IsFasterThan(const Locator& locator, bool ignoreLegacyDiffSrc) const;
    std::pair<uint32_t, uint32_t> GetLocatorRange() const;

    bool ShrinkToRange(uint32_t from, uint32_t to);
    void Reset();
    void SetLegacyLocator() { _isLegacyLocator = true; }
    bool IsLegacyLocator() const { return _isLegacyLocator; }

    bool TEST_IsLegal() const;

private:
    struct CompareStruct {
        CompareStruct(const base::ProgressVector* progress) : progressPtr(progress)
        {
            if (progress->size() > 0) {
                from = (*progress)[0].from;
            }
        }
        bool IsEof() { return cursor >= progressPtr->size(); }
        base::Progress GetCurrentProgress()
        {
            auto progress = (*progressPtr)[cursor];
            progress.from = from;
            return progress;
        }
        base::Progress::Offset GetCurrentOffset()
        {
            assert(!IsEof());
            return (*progressPtr)[cursor].offset;
        }
        void MoveToNext()
        {
            cursor++;
            if (!IsEof()) {
                from = (*progressPtr)[cursor].from;
            }
        }
        const base::ProgressVector* progressPtr;
        size_t cursor = 0;
        uint32_t from = 0;
    };

private:
    int32_t calculateSerializeVersion() const;
    std::string SerializeV1() const;
    std::string SerializeV0() const;
    size_t ProgressSize(size_t count) const;

    size_t UserDataSize(size_t len) const
    {
        if (len == 0) {
            return 0;
        }
        return sizeof(int64_t) + len;
    }
    std::pair<bool, base::Progress> GetLeftProgress(CompareStruct& currentProgress, CompareStruct& updateProgress);
    void MergeProgress(base::ProgressVector& mergeProgress);
    bool UpdateProgressVec(size_t i, const base::ProgressVector& progressVec);
    bool IsFullyFasterThan(const Locator& other) const;
    bool IsFullyFasterThan(const base::ProgressVector& progressVec, const base::ProgressVector& otherProgressVec) const;
    bool DeserializeProgressV0(const char*& data, const char* end);

private:
    bool DeserializeV0(const char* str, const char* end);
    bool DeserializeV1(const char* str, const char* end);
    bool Deserialize(const char* str, size_t size);
    uint64_t _src = 0;
    base::Progress::Offset _minOffset;
    base::MultiProgress _multiProgress;
    std::string _userData;
    bool _isLegacyLocator = false;
};

class ThreadSafeLocatorGuard
{
public:
    ThreadSafeLocatorGuard() {}
    ThreadSafeLocatorGuard(const ThreadSafeLocatorGuard& other) { _locator = other._locator; }
    void operator=(const ThreadSafeLocatorGuard& other) { _locator = other._locator; }

public:
    void Update(const Locator& locator)
    {
        autil::ScopedLock lock(_lock);
        _locator.Update(locator);
    }
    void Set(const Locator& locator)
    {
        autil::ScopedLock lock(_lock);
        _locator = locator;
    }
    Locator Get() const
    {
        autil::ScopedLock lock(_lock);
        return _locator;
    }

private:
    Locator _locator;
    mutable autil::ThreadMutex _lock;
};

} // namespace indexlibv2::framework
