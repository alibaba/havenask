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

#include <inttypes.h>
#include <limits>
#include <string>
#include <string_view>

#include "autil/ConstString.h"
#include "autil/Lock.h"
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

    Locator() : _src(0), _minOffset(base::Progress::INVALID_OFFSET) {}
    // TODO(tianxiao) remove structor
    Locator(uint64_t src, int64_t minOffset) : _src(src), _minOffset({minOffset, 0})
    {
        _progress.emplace_back(base::Progress(_minOffset));
    }
    Locator(uint64_t src, const base::Progress::Offset minOffset) : _src(src), _minOffset(minOffset)
    {
        _progress.emplace_back(base::Progress(_minOffset));
    }

    Locator(const Locator& other)
    {
        if (&other == this) {
            return;
        }
        this->_src = other._src;
        this->_minOffset = other._minOffset;
        this->_progress = other._progress;
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
        this->_progress = other._progress;
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
    // void SetOffset(int64_t offset)
    // {
    //     _minOffset = {offset, 0};
    //     _progress.clear();
    //     _progress.emplace_back(base::Progress(_minOffset));
    // }
    void SetOffset(const base::Progress::Offset& offset)
    {
        _minOffset = offset;
        _progress.clear();
        _progress.emplace_back(base::Progress(offset));
    }
    base::Progress::Offset GetOffset() const { return _minOffset; }
    base::Progress::Offset GetMaxOffset() const
    {
        base::Progress::Offset ret = base::Progress::INVALID_OFFSET;
        for (const auto& progress : _progress) {
            ret = std::max(ret, progress.offset);
        }
        return ret;
    }

    void SetProgress(const std::vector<base::Progress>& progress)
    {
        if (progress.empty()) {
            return;
        }
        _progress = progress;
        _minOffset = _progress[0].offset;
        for (size_t i = 1; i < progress.size(); ++i) {
            if (progress[i].offset < _minOffset) {
                _minOffset = progress[i].offset;
            }
        }
    }
    const std::vector<base::Progress>& GetProgress() const { return _progress; }

    void SetUserData(std::string userData) { _userData = std::move(userData); }
    const std::string& GetUserData() const { return _userData; }

    size_t Size() const;
    std::string DebugString() const;
    bool IsValid() const;
    Locator::LocatorCompareResult IsFasterThan(uint16_t hashId, const base::Progress::Offset& offset) const;
    Locator::LocatorCompareResult IsFasterThan(const Locator& locator, bool ignoreLegacyDiffSrc) const;
    std::pair<uint32_t, uint32_t> GetLocatorRange() const;

    bool ShrinkToRange(uint32_t from, uint32_t to);
    void Reset();
    void SetLegacyLocator() { _isLegacyLocator = true; }
    bool IsLegacyLocator() const { return _isLegacyLocator; }

    bool TEST_IsLegal() const;

private:
    struct CompareStruct {
        CompareStruct(const std::vector<base::Progress>* progress) : progressPtr(progress)
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
        const std::vector<base::Progress>* progressPtr;
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
    void MergeProgress(std::vector<base::Progress>& mergeProgress);
    bool IsFullyFasterThan(const Locator& other) const;
    bool DeserializeProgressV0(const char*& data, const char* end);

private:
    bool DeserializeV0(const char* str, const char* end);
    bool DeserializeV1(const char* str, const char* end);
    bool Deserialize(const char* str, size_t size);
    uint64_t _src = 0;
    base::Progress::Offset _minOffset;
    std::vector<base::Progress> _progress;
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
