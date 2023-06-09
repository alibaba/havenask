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
#include "indexlib/framework/Locator.h"

#include <iostream>

namespace indexlibv2::framework {

std::pair<bool, base::Progress> Locator::GetLeftProgress(CompareStruct& currentProgress, CompareStruct& updateProgress)
{
    auto current = currentProgress.GetCurrentProgress();
    auto update = updateProgress.GetCurrentProgress();
    if (current.from < update.from) {
        if (current.to < update.from) {
            // current : |---|
            // update  :       |---|
            currentProgress.MoveToNext();
        } else {
            // current : |---|
            // update  :    |---|
            current.to = update.from - 1;
            currentProgress.from = update.from;
        }
        return {true, current};
    } else if (current.from > update.from) {
        if (update.to < current.from) {
            // current :       |---|
            // update  : |---|
            updateProgress.MoveToNext();
        } else {
            // current :    |---|
            // update  : |---|
            update.to = current.from - 1;
            updateProgress.from = current.from;
        }
        return {true, update};
    } else {
        int64_t updateOffset = updateProgress.GetCurrentOffset();
        int64_t currentOffset = currentProgress.GetCurrentOffset();
        if (updateOffset != -1 && updateOffset < currentOffset) {
            return {false, base::Progress()};
        }
        if (update.to < current.to) {
            // current : |--------|
            // update  : |-----|
            updateProgress.MoveToNext();
            currentProgress.from = update.to + 1;
            return {true, update};
        } else if (update.to > current.to) {
            // current : |-----|
            // update  : |--------|
            currentProgress.MoveToNext();
            current.offset = updateOffset;
            updateProgress.from = current.to + 1;
            return {true, current};
        } else {
            updateProgress.MoveToNext();
            currentProgress.MoveToNext();
            return {true, update};
        }
    }
    assert(false);
    return {false, base::Progress()};
}

void Locator::MergeProgress(std::vector<base::Progress>& mergeProgress)
{
    std::vector<base::Progress> mergeResult;
    for (const auto& progress : mergeProgress) {
        if (mergeResult.empty()) {
            mergeResult.push_back(progress);
            continue;
        }

        size_t idx = mergeResult.size() - 1;
        if (mergeResult[idx].offset == progress.offset && mergeResult[idx].to + 1 == progress.from) {
            mergeResult[idx].to = progress.to;
            continue;
        }
        mergeResult.push_back(progress);
    }
    mergeProgress.swap(mergeResult);
}

bool Locator::Update(const Locator& other)
{
    if (other._src != _src) {
        // new src locator, just replace it
        *this = other;
        return true;
    }

    if (other._progress.size() == 1 && _progress.size() == 1 && other._progress[0].from == _progress[0].from &&
        other._progress[0].to == _progress[0].to) {
        if (other._progress[0].offset < _progress[0].offset) {
            return false;
        }
        _progress[0].offset = other._progress[0].offset;
        _minOffset = other._progress[0].offset;
        _userData = other._userData;
        return true;
    }
    CompareStruct currentProgress(&_progress);
    CompareStruct updateProgress(&(other._progress));

    std::vector<base::Progress> updateResult;
    while (!currentProgress.IsEof() && !updateProgress.IsEof()) {
        auto [status, progress] = GetLeftProgress(currentProgress, updateProgress);
        if (!status) {
            return false;
        }
        updateResult.push_back(progress);
    }
    while (!currentProgress.IsEof()) {
        auto progress = currentProgress.GetCurrentProgress();
        updateResult.push_back(progress);
        currentProgress.MoveToNext();
    }
    while (!updateProgress.IsEof()) {
        auto progress = updateProgress.GetCurrentProgress();
        updateResult.push_back(progress);
        updateProgress.MoveToNext();
    }
    MergeProgress(updateResult);
    _progress.swap(updateResult);

    _minOffset = -1;
    for (const auto& progress : _progress) {
        if (progress.offset == -1) {
            // TODO: by yijie.zhang delete -1 offset
            continue;
        }
        if (_minOffset == -1) {
            _minOffset = progress.offset;
        } else {
            _minOffset = std::min(_minOffset, progress.offset);
        }
    }
    _userData = other._userData;
    return true;
}

bool Locator::IsFullyFasterThan(const Locator& other) const
{
    bool isFullyFaster = true;
    for (auto otherProgress : other._progress) {
        bool hasFullyFaster = false;
        for (const auto& currentProgress : _progress) {
            if (otherProgress.offset <= 0) {
                hasFullyFaster = true;
                break;
            }
            if (currentProgress.from > otherProgress.from) {
                break;
            }
            if (currentProgress.to < otherProgress.from) {
                continue;
            }

            if (currentProgress.to >= otherProgress.to) {
                if (currentProgress.offset >= otherProgress.offset) {
                    hasFullyFaster = true;
                }
                break;
            }

            if (currentProgress.offset < otherProgress.offset) {
                break;
            }

            otherProgress.from = currentProgress.to + 1;
        }
        if (!hasFullyFaster) {
            isFullyFaster = false;
            break;
        }
    }
    return isFullyFaster;
}

std::string Locator::Serialize() const
{
    std::string result;
    result.resize(Size());
    char* data = result.data();
    *(uint64_t*)data = _src;
    data += sizeof(_src);
    *(int64_t*)data = _minOffset;
    data += sizeof(_minOffset);

    *(uint64_t*)data = (uint64_t)_progress.size();
    data += sizeof(uint64_t);
    for (int i = 0; i < _progress.size(); i++) {
        *(int64_t*)data = _progress[i].offset;
        data += sizeof(int64_t);
        *(uint32_t*)data = _progress[i].from;
        data += sizeof(uint32_t);
        *(uint32_t*)data = _progress[i].to;
        data += sizeof(uint32_t);
    }

    if (!_userData.empty()) {
        *(uint64_t*)data = _userData.size();
        data += sizeof(uint64_t);
        memcpy(data, _userData.data(), _userData.size());
        data += _userData.size();
    }

    return result;
}

bool Locator::DeserializeProgress(const char*& data, const char* end)
{
    const char* originData = data;
    uint64_t progressSize = 0;
    if (data + sizeof(progressSize) > end) {
        data = originData;
        return false;
    }
    progressSize = *(uint64_t*)data;
    data += sizeof(progressSize);

    if (data + progressSize * sizeof(base::Progress) > end) {
        data = originData;
        return false;
    }
    _progress.reserve(progressSize);
    for (size_t i = 0; i < progressSize; i++) {
        base::Progress singleProgress;
        singleProgress.offset = *(int64_t*)data;
        data += sizeof(int64_t);
        singleProgress.from = *(uint32_t*)data;
        data += sizeof(uint32_t);
        singleProgress.to = *(uint32_t*)data;
        data += sizeof(uint32_t);
        if (singleProgress.offset < _minOffset || singleProgress.from < 0 || singleProgress.from > 65535 ||
            singleProgress.to < 0 || singleProgress.to > 65535) {
            _progress.clear();
            data = originData;
            return false;
        }
        _progress.emplace_back(singleProgress);
    }
    return true;
}

bool Locator::Deserialize(const char* data, size_t size)
{
    Reset();
    const char* end = data + size;
    if (data == nullptr || size < sizeof(_src) + sizeof(_minOffset)) {
        return false;
    }
    _src = *(uint64_t*)data;
    data += sizeof(_src);
    _minOffset = *(int64_t*)data;
    data += sizeof(_minOffset);

    if (data == end) {
        base::Progress singleProgress;
        singleProgress.from = 0;
        singleProgress.to = 65535;
        singleProgress.offset = _minOffset;
        _progress.emplace_back(singleProgress);
        return true;
    }
    if (!DeserializeProgress(data, end)) {
        // deserialize progress failed, legacy code for compatible with old user data format
        base::Progress singleProgress;
        singleProgress.from = 0;
        singleProgress.to = 65535;
        singleProgress.offset = _minOffset;
        _progress.emplace_back(singleProgress);
        _userData.append(data, end - data);
        return true;
    }
    if (data == end) {
        return true;
    }
    uint64_t userDataLen = 0;
    if (data + sizeof(userDataLen) > end) {
        Reset();
        return false;
    }
    userDataLen = *(uint64_t*)data;
    data += sizeof(userDataLen);
    if (data + userDataLen != end) {
        Reset();
        return false;
    }
    if (userDataLen > 0) {
        _userData.append(data, userDataLen);
    }
    return true;
}

Locator::LocatorCompareResult Locator::IsFasterThan(uint16_t hashId, int64_t timestamp) const
{
    for (size_t i = 0; i < _progress.size(); i++) {
        if (_progress[i].from <= hashId && hashId <= _progress[i].to) {
            if (_progress[i].offset > timestamp) {
                return LocatorCompareResult::LCR_FULLY_FASTER;
            } else {
                return LocatorCompareResult::LCR_SLOWER;
            }
        }
    }
    return LocatorCompareResult::LCR_SLOWER;
}

bool Locator::IsSameSrc(const Locator& otherLocator, bool ignoreLegacyDiffSrc) const
{
    if (ignoreLegacyDiffSrc && (_isLegacyLocator || otherLocator._isLegacyLocator)) {
        return true;
    }
    return _src == otherLocator._src;
}

Locator::LocatorCompareResult Locator::IsFasterThan(const Locator& locator, bool ignoreLegacyDiffSrc) const
{
    if (!IsSameSrc(locator, ignoreLegacyDiffSrc)) {
        return LocatorCompareResult::LCR_INVALID;
    }
    if (ignoreLegacyDiffSrc && (IsLegacyLocator() || locator.IsLegacyLocator())) {
        if (IsLegacyLocator()) {
            int64_t maxOffset = -1;
            int64_t minOffset = locator.GetOffset();
            for (const auto& progress : locator.GetProgress()) {
                maxOffset = std::max(maxOffset, progress.offset);
            }
            if (_minOffset < minOffset) {
                return LocatorCompareResult::LCR_SLOWER;
            }
            if (_minOffset >= maxOffset) {
                return LocatorCompareResult::LCR_FULLY_FASTER;
            }
            return LocatorCompareResult::LCR_PARTIAL_FASTER;
        } else {
            int64_t maxOffset = -1;
            int64_t minOffset = _minOffset;
            for (const auto& progress : _progress) {
                maxOffset = std::max(maxOffset, progress.offset);
            }
            if (locator.GetOffset() <= minOffset) {
                return LocatorCompareResult::LCR_FULLY_FASTER;
            }
            if (locator.GetOffset() > maxOffset) {
                return LocatorCompareResult::LCR_SLOWER;
            }
            return LocatorCompareResult::LCR_PARTIAL_FASTER;
        }
    }
    // TODO(tianxiao.ttx) locator is invalid
    if (!locator.IsValid()) {
        return LocatorCompareResult::LCR_FULLY_FASTER;
    }
    if (!IsValid()) {
        return LocatorCompareResult::LCR_SLOWER;
    }
    bool isFullyFaster = IsFullyFasterThan(locator);
    if (isFullyFaster) {
        return LocatorCompareResult::LCR_FULLY_FASTER;
    }
    bool isFullySlower = locator.IsFullyFasterThan(*this);
    if (isFullySlower) {
        return LocatorCompareResult::LCR_SLOWER;
    }
    return LocatorCompareResult::LCR_PARTIAL_FASTER;
}

bool Locator::ShrinkToRange(uint32_t from, uint32_t to)
{
    std::vector<base::Progress> newProgress;
    if (_progress[0].from > from || _progress[_progress.size() - 1].to < to) {
        return false;
    }
    for (const auto& singleProgress : _progress) {
        if (singleProgress.to < from || singleProgress.from > to) {
            continue;
        }
        newProgress.push_back(base::Progress(std::max(singleProgress.from, from), std::min(singleProgress.to, to),
                                             singleProgress.offset));
    }
    _progress.swap(newProgress);
    return true;
}

// TODO(xiaohao.yxh): add progress
std::string Locator::DebugString() const
{
    std::string debugString = std::to_string(_src) + ":" + std::to_string(_minOffset);
    if (!_userData.empty()) {
        debugString += ":" + _userData;
    }
    debugString += ":[";
    for (const auto& progress : _progress) {
        debugString += "{" + std::to_string(progress.from) + "," + std::to_string(progress.to) + "," +
                       std::to_string(progress.offset) + "}";
    }
    debugString += "]";
    return debugString;
}

std::pair<uint32_t, uint32_t> Locator::GetLocatorRange() const
{
    uint32_t from = 65535;
    uint32_t to = 0;
    for (const auto& singleProgress : _progress) {
        from = std::min(from, singleProgress.from);
        to = std::max(to, singleProgress.to);
    }
    return {from, to};
}

void Locator::Reset()
{
    _src = 0;
    _minOffset = -1;
    _progress.clear();
    _userData.clear();
}

bool Locator::IsValid() const
{
    if (_minOffset >= 0) {
        return true;
    }
    for (auto& p : _progress) {
        if (p.offset >= 0) {
            return true;
        }
    }
    return false;
}
} // namespace indexlibv2::framework
