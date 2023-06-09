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

#include <memory>

#include "indexlib/index/common/numeric_compress/EquivalentCompressReader.h"
#include "indexlib/index/common/numeric_compress/EquivalentCompressWriter.h"
#include "indexlib/util/Status2Exception.h"

namespace indexlibv2::index {

template <typename T, typename Compressor = indexlib::index::EquivalentCompressWriter<T>>
class EqualValueCompressAdvisor
{
public:
    EqualValueCompressAdvisor() {}
    ~EqualValueCompressAdvisor() {}

public:
    static uint32_t GetOptSlotItemCount(const indexlib::index::EquivalentCompressReader<T>& reader, size_t& optCompLen);

    static std::pair<Status, uint32_t>
    EstimateOptimizeSlotItemCount(const indexlib::index::EquivalentCompressReader<T>& reader, uint32_t sampleRatio);

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP_TEMPLATE_2(indexlib.index, EqualValueCompressAdvisor, T1, T2);

template <typename T>
class SampledItemIterator
{
public:
    SampledItemIterator(const indexlib::index::EquivalentCompressReader<T>& reader, const uint32_t sampleRatio,
                        const uint32_t maxStepLen = 1024)
        : _reader(reader)
        , _beginOffset(0)
        , _offsetInBlock(0)
        , _blockSize(maxStepLen)
        , _stepedBlocks(0)
    {
        assert(reader.Size() > 0);
        uint32_t totalBlocks = (reader.Size() + maxStepLen - 1) / maxStepLen;
        uint32_t sampleBlockCnt = totalBlocks * sampleRatio / 100;
        if (sampleBlockCnt > 1) {
            _stepedBlocks = (totalBlocks - 1) / (sampleBlockCnt - 1);
        } else {
            _stepedBlocks = 1;
        }
    }

    bool HasNext() const { return _beginOffset + _offsetInBlock < _reader.Size(); }

    std::pair<Status, T> Next()
    {
        assert(_beginOffset + _offsetInBlock < _reader.Size());
        auto [status, value] = _reader[_beginOffset + _offsetInBlock];
        RETURN2_IF_STATUS_ERROR(status, 0, "read data from EqualValueCompressReader fail");

        if (++_offsetInBlock == _blockSize) {
            _beginOffset += _blockSize * _stepedBlocks;
            _offsetInBlock = 0;
        }
        return std::make_pair(Status::OK(), value);
    }

private:
    const indexlib::index::EquivalentCompressReader<T>& _reader;
    uint32_t _beginOffset;
    uint32_t _offsetInBlock;
    uint32_t _blockSize;
    uint32_t _stepedBlocks;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, SampledItemIterator, T);
/////////////////////////////////////////////////////////////
template <typename T, typename Compressor>
inline uint32_t EqualValueCompressAdvisor<T, Compressor>::GetOptSlotItemCount(
    const indexlib::index::EquivalentCompressReader<T>& reader, size_t& optCompLen)
{
    assert(reader.Size() > 0);

    // 64 ~ 1024
    uint32_t option[] = {1 << 6, 1 << 7, 1 << 8, 1 << 9, 1 << 10};

    uint32_t i = 0;
    auto [status, minCompressLen] = Compressor::CalculateCompressLength(reader, option[0]);
    indexlib::util::ThrowIfStatusError(status);

    for (i = 1; i < sizeof(option) / sizeof(uint32_t); ++i) {
        auto [innerStatus, compLen] = Compressor::CalculateCompressLength(reader, option[i]);
        indexlib::util::ThrowIfStatusError(innerStatus);
        if (compLen <= minCompressLen) {
            minCompressLen = compLen;
            continue;
        }
        break;
    }

    optCompLen = minCompressLen;
    return option[i - 1];
}

template <typename T, typename Compressor>
inline std::pair<Status, uint32_t> EqualValueCompressAdvisor<T, Compressor>::EstimateOptimizeSlotItemCount(
    const indexlib::index::EquivalentCompressReader<T>& reader, uint32_t sampleRatio)
{
    assert(reader.Size() > 0);
    // 64 ~ 1024
    uint32_t option[] = {1 << 6, 1 << 7, 1 << 8, 1 << 9, 1 << 10};
    uint32_t i = 0;
    SampledItemIterator<T> iter(reader, sampleRatio, /*maxStepLen*/ 1024);

    auto [status, minCompressLen] = Compressor::CalculateCompressLength(iter, option[0]);
    RETURN2_IF_STATUS_ERROR(status, 0, "calculate compress length fail");
    for (i = 1; i < sizeof(option) / sizeof(uint32_t); ++i) {
        SampledItemIterator<T> it(reader, sampleRatio, /*maxStepLen*/ 1024);
        auto [innerStatus, compLen] = Compressor::CalculateCompressLength(it, option[i]);
        RETURN2_IF_STATUS_ERROR(innerStatus, 0, "calculate compress length fail");
        if (compLen <= minCompressLen) {
            minCompressLen = compLen;
            continue;
        }
        break;
    }
    return std::make_pair(Status::OK(), option[i - 1]);
}
} // namespace indexlibv2::index
