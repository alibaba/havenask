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
    static_assert(sizeof(base::Progress) == 16, "Progress has been modified, deserialize may failed");
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

    Locator() : _src(0), _minOffset(-1) {}
    Locator(uint64_t src, int64_t minOffset) : _src(src), _minOffset(minOffset)
    {
        _progress.emplace_back(base::Progress(minOffset));
    }
    // other locator muster fully faster than current locator, or return false
    bool Update(const Locator& other);
    std::string Serialize() const;
    bool Deserialize(const std::string& str) { return Deserialize(str.data(), str.size()); }
    bool Deserialize(const autil::StringView& str) { return Deserialize(str.data(), str.size()); }
    void SetSrc(uint64_t src) { _src = src; }
    uint64_t GetSrc() const { return _src; }

    bool IsSameSrc(const Locator& otherLocator, bool ignoreLegacyDiffSrc) const;
    void SetOffset(int64_t offset)
    {
        _minOffset = offset;
        _progress.clear();
        _progress.emplace_back(base::Progress(offset));
    }
    int64_t GetOffset() const { return _minOffset; }
    int64_t GetMaxOffset() const
    {
        int64_t ret = -1;
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

    size_t Size() const
    {
        return sizeof(_src) + sizeof(_minOffset) + ProgressSize(_progress.size()) + UserDataSize(_userData.size());
    }
    std::string DebugString() const;
    bool IsValid() const;
    Locator::LocatorCompareResult IsFasterThan(uint16_t hashId, int64_t timestamp) const;
    Locator::LocatorCompareResult IsFasterThan(const Locator& locator, bool ignoreLegacyDiffSrc) const;
    std::pair<uint32_t, uint32_t> GetLocatorRange() const;

    bool ShrinkToRange(uint32_t from, uint32_t to);
    void Reset();
    void SetLegacyLocator() { _isLegacyLocator = true; }
    bool IsLegacyLocator() const { return _isLegacyLocator; }

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
        int64_t GetCurrentOffset()
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
    size_t ProgressSize(size_t count) const { return sizeof(uint64_t) + sizeof(base::Progress) * count; }
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
    bool DeserializeProgress(const char*& data, const char* end);

private:
    bool Deserialize(const char* str, size_t size);
    uint64_t _src = 0;
    int64_t _minOffset = -1;
    std::vector<base::Progress> _progress;
    std::string _userData;
    bool _isLegacyLocator = false;
};

class ThreadSafeLocatorGuard
{
public:
    ThreadSafeLocatorGuard() {}
    ThreadSafeLocatorGuard(const ThreadSafeLocatorGuard& other) { mLocator = other.mLocator; }
    void operator=(const ThreadSafeLocatorGuard& other) { mLocator = other.mLocator; }

public:
    void Update(const Locator& locator)
    {
        autil::ScopedLock lock(mLock);
        mLocator.Update(locator);
    }
    void Set(const Locator& locator)
    {
        autil::ScopedLock lock(mLock);
        mLocator = locator;
    }
    Locator Get() const
    {
        autil::ScopedLock lock(mLock);
        return mLocator;
    }

private:
    Locator mLocator;
    mutable autil::ThreadMutex mLock;
};

} // namespace indexlibv2::framework
