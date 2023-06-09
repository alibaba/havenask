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
#ifndef __INDEXLIB_EQUAL_VALUE_COMPRESS_ADVISOR_H
#define __INDEXLIB_EQUAL_VALUE_COMPRESS_ADVISOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/common/numeric_compress/EquivalentCompressWriter.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Status2Exception.h"

namespace indexlib { namespace index {

#define INIT_SLOT_ITEM_COUNT 64

template <typename T, typename Compressor = EquivalentCompressWriter<T>>
class EqualValueCompressAdvisor
{
public:
    EqualValueCompressAdvisor() {}
    ~EqualValueCompressAdvisor() {}

public:
    static uint32_t GetOptSlotItemCount(const EquivalentCompressReader<T>& reader, size_t& optCompLen);

    static uint32_t EstimateOptimizeSlotItemCount(const EquivalentCompressReader<T>& reader, uint32_t sampleRatio = 10);
};

template <typename T>
class SampledItemIterator
{
public:
    SampledItemIterator(const EquivalentCompressReader<T>& reader, const uint32_t sampleRatio,
                        const uint32_t maxStepLen = 1024)
        : mReader(reader)
        , mBeginOffset(0)
        , mOffsetInBlock(0)
        , mBlockSize(maxStepLen)
        , mStepedBlocks(0)
    {
        assert(reader.Size() > 0);
        uint32_t totalBlocks = (reader.Size() + maxStepLen - 1) / maxStepLen;
        uint32_t sampleBlockCnt = totalBlocks * sampleRatio / 100;
        if (sampleBlockCnt > 1) {
            mStepedBlocks = (totalBlocks - 1) / (sampleBlockCnt - 1);
        } else {
            mStepedBlocks = 1;
        }
    }

    bool HasNext() const { return mBeginOffset + mOffsetInBlock < mReader.Size(); }

    std::pair<Status, T> Next()
    {
        assert(mBeginOffset + mOffsetInBlock < mReader.Size());
        auto value = mReader[mBeginOffset + mOffsetInBlock];

        if (++mOffsetInBlock == mBlockSize) {
            mBeginOffset += mBlockSize * mStepedBlocks;
            mOffsetInBlock = 0;
        }
        return value;
    }

private:
    const EquivalentCompressReader<T>& mReader;
    uint32_t mBeginOffset;
    uint32_t mOffsetInBlock;
    uint32_t mBlockSize;
    uint32_t mStepedBlocks;
};

/////////////////////////////////////////////////////////////
template <typename T, typename Compressor>
inline uint32_t EqualValueCompressAdvisor<T, Compressor>::GetOptSlotItemCount(const EquivalentCompressReader<T>& reader,
                                                                              size_t& optCompLen)
{
    assert(reader.Size() > 0);

    // 64 ~ 1024
    uint32_t option[] = {1 << 6, 1 << 7, 1 << 8, 1 << 9, 1 << 10};

    uint32_t i = 0;
    auto [status, minCompressLen] = Compressor::CalculateCompressLength(reader, option[0]);
    indexlib::util::ThrowIfStatusError(status);
    for (i = 1; i < sizeof(option) / sizeof(uint32_t); ++i) {
        auto [status, compLen] = Compressor::CalculateCompressLength(reader, option[i]);
        indexlib::util::ThrowIfStatusError(status);
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
inline uint32_t
EqualValueCompressAdvisor<T, Compressor>::EstimateOptimizeSlotItemCount(const EquivalentCompressReader<T>& reader,
                                                                        uint32_t sampleRatio)
{
    assert(reader.Size() > 0);
    // 64 ~ 1024
    uint32_t option[] = {1 << 6, 1 << 7, 1 << 8, 1 << 9, 1 << 10};
    uint32_t i = 0;
    SampledItemIterator<T> iter(reader, sampleRatio);

    auto [status, minCompressLen] = Compressor::CalculateCompressLength(iter, option[0]);
    indexlib::util::ThrowIfStatusError(status);
    for (i = 1; i < sizeof(option) / sizeof(uint32_t); ++i) {
        SampledItemIterator<T> it(reader, sampleRatio);
        auto [status, compLen] = Compressor::CalculateCompressLength(it, option[i]);
        indexlib::util::ThrowIfStatusError(status);
        if (compLen <= minCompressLen) {
            minCompressLen = compLen;
            continue;
        }
        break;
    }
    return option[i - 1];
}
}} // namespace indexlib::index

#endif //__INDEXLIB_EQUAL_VALUE_COMPRESS_ADVISOR_H
