#ifndef __INDEXLIB_EQUAL_VALUE_COMPRESS_ADVISOR_H
#define __INDEXLIB_EQUAL_VALUE_COMPRESS_ADVISOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/numeric_compress/equivalent_compress_writer.h"

IE_NAMESPACE_BEGIN(index);

#define INIT_SLOT_ITEM_COUNT 64

template <typename T, typename Compressor = common::EquivalentCompressWriter<T> >
class EqualValueCompressAdvisor
{
public:
    EqualValueCompressAdvisor() {}
    ~EqualValueCompressAdvisor() {}

public:
    static uint32_t GetOptSlotItemCount( 
            const common::EquivalentCompressReader<T>& reader, 
            size_t &optCompLen);

    static uint32_t EstimateOptimizeSlotItemCount(
            const common::EquivalentCompressReader<T>& reader,
            uint32_t sampleRatio = 10);
};


template<typename T>
class SampledItemIterator
{
public:
    SampledItemIterator(const common::EquivalentCompressReader<T>& reader, 
                        const uint32_t sampleRatio, 
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
        if (sampleBlockCnt > 1)
        {
            mStepedBlocks = (totalBlocks - 1) / (sampleBlockCnt - 1);
        }
        else
        {
            mStepedBlocks = 1;
        }
    }

    bool HasNext() const
    {
        return mBeginOffset + mOffsetInBlock < mReader.Size();
    }

    T Next()
    {
        assert(mBeginOffset + mOffsetInBlock < mReader.Size());
        T value = mReader[mBeginOffset + mOffsetInBlock];

        if (++mOffsetInBlock == mBlockSize)
        {
            mBeginOffset += mBlockSize *mStepedBlocks;
            mOffsetInBlock = 0;
        }
        return value;
    }

private:
    const common::EquivalentCompressReader<T>& mReader;
    uint32_t mBeginOffset;
    uint32_t mOffsetInBlock;
    uint32_t mBlockSize;
    uint32_t mStepedBlocks;
};

/////////////////////////////////////////////////////////////
template <typename T, typename Compressor>
inline uint32_t EqualValueCompressAdvisor<T, Compressor>::GetOptSlotItemCount(
        const common::EquivalentCompressReader<T>& reader,
        size_t &optCompLen)
{
    assert(reader.Size() > 0);

    // 64 ~ 1024
    uint32_t option[] = { 1<<6,  1<<7,  1<<8,  1<<9, 1<<10 };
    
    uint32_t i = 0;
    size_t minCompressLen = 
        Compressor::CalculateCompressLength(reader, option[0]);

    for (i = 1; i < sizeof(option) / sizeof(uint32_t); ++i)
    {
        size_t compLen = Compressor::CalculateCompressLength(reader, option[i]);
        if (compLen <= minCompressLen)
        {
            minCompressLen = compLen;
            continue;
        }
        break;
    }

    optCompLen = minCompressLen;
    return option[i - 1];
}

template <typename T, typename Compressor>
inline uint32_t EqualValueCompressAdvisor<T, Compressor>::EstimateOptimizeSlotItemCount(
        const common::EquivalentCompressReader<T>& reader, uint32_t sampleRatio)
{
    assert(reader.Size() > 0);
    // 64 ~ 1024
    uint32_t option[] = { 1<<6,  1<<7,  1<<8,  1<<9, 1<<10 };
    uint32_t i = 0;
    SampledItemIterator<T> iter(reader, sampleRatio);
    
    size_t minCompressLen = Compressor::CalculateCompressLength(iter, option[0]);    
    for (i = 1; i < sizeof(option) / sizeof(uint32_t); ++i)
    {
        SampledItemIterator<T> it(reader, sampleRatio);
        size_t compLen = Compressor::CalculateCompressLength(it, option[i]);
        if (compLen <= minCompressLen)
        {
            minCompressLen = compLen;
            continue;
        }
        break;
    }
    return option[i-1];
}


IE_NAMESPACE_END(index);

#endif //__INDEXLIB_EQUAL_VALUE_COMPRESS_ADVISOR_H
