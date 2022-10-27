#ifndef __INDEXLIB_ADAPTIVE_ATTRIBUTE_OFFSET_DUMPER_H
#define __INDEXLIB_ADAPTIVE_ATTRIBUTE_OFFSET_DUMPER_H

#include <tr1/memory>
#include <autil/mem_pool/pool_allocator.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/equal_value_compress_dumper.h"

DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(file_system, FileWriter);

IE_NAMESPACE_BEGIN(index);

class AdaptiveAttributeOffsetDumper
{
private:
    template<typename T>
    struct OffsetTypes
    {
        typedef autil::mem_pool::pool_allocator<T> Allocator;
        typedef std::vector<T, Allocator> Vector;
        typedef std::tr1::shared_ptr<Vector> VectorPtr;
    };
    typedef OffsetTypes<uint64_t>::Vector U64Vector;
    typedef OffsetTypes<uint32_t>::Vector U32Vector;

public:
    AdaptiveAttributeOffsetDumper(autil::mem_pool::PoolBase* pool);
    ~AdaptiveAttributeOffsetDumper();

public:
    void Init(const config::AttributeConfigPtr& attrConf);

    void PushBack(uint64_t offset);
    uint64_t GetOffset(size_t pos) const;
    void SetOffset(size_t pos, uint64_t offset);

    bool IsU64Offset() const;
    size_t Size() const;
    void Reserve(size_t reserveCount);

    void Clear();

    void Dump(const file_system::FileWriterPtr& offsetFile);

private:
    template <typename OffsetType, typename OffsetFilePtr>
    void WriteOffsetData(const typename OffsetTypes<OffsetType>::Vector& offsetVec,
                         const OffsetFilePtr& offsetFile)
    {
        if (mEnableCompress)
        {
            EqualValueCompressDumper<OffsetType> compressDumper(true, mPool);
            compressDumper.CompressData((OffsetType*)offsetVec.data(), offsetVec.size());
            compressDumper.Dump(offsetFile);
        }
        else
        {
            offsetFile->Write((void*)(&(*(offsetVec.begin()))), 
                    sizeof(OffsetType) * offsetVec.size());
        }
    }

private:
    autil::mem_pool::PoolBase* mPool;
    bool mEnableCompress;
    uint64_t mOffsetThreshold;
    uint32_t mReserveCount;

    OffsetTypes<uint32_t>::Allocator mU32Allocator;
    OffsetTypes<uint64_t>::Allocator mU64Allocator;
    U32Vector mU32Offsets;
    OffsetTypes<uint64_t>::VectorPtr mU64Offsets;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AdaptiveAttributeOffsetDumper);
///////////////////////////////////////////////////////

inline void AdaptiveAttributeOffsetDumper::PushBack(uint64_t offset)
{
    if (mU64Offsets)
    {
        mU64Offsets->push_back(offset);
        return;
    }

    if (offset <= mOffsetThreshold)
    {
        mU32Offsets.push_back((uint32_t)offset);
        return;
    }

    mU64Offsets.reset(new U64Vector(mU64Allocator));
    if (mReserveCount > 0)
    {
        mU64Offsets->reserve(mReserveCount);
    }

    for (size_t i = 0; i < mU32Offsets.size(); i++)
    {
        mU64Offsets->push_back(mU32Offsets[i]);
    }
    mU64Offsets->push_back(offset);

    U32Vector(mU32Allocator).swap(mU32Offsets);
}

inline uint64_t AdaptiveAttributeOffsetDumper::GetOffset(size_t pos) const
{
    assert(pos < Size());

    if (mU64Offsets)
    {
        return (*mU64Offsets)[pos];
    }
    return mU32Offsets[pos];
}

inline void AdaptiveAttributeOffsetDumper::SetOffset(size_t pos, uint64_t offset)
{
    assert(pos < Size());

    if (mU64Offsets)
    {
        (*mU64Offsets)[pos] = offset;
        return;
    }

    if (offset <= mOffsetThreshold)
    {
        mU32Offsets[pos] = (uint32_t)offset;
        return;
    }

    mU64Offsets.reset(new U64Vector(mU64Allocator));
    if (mReserveCount > 0)
    {
        mU64Offsets->reserve(mReserveCount);
    }

    for (size_t i = 0; i < mU32Offsets.size(); i++)
    {
        mU64Offsets->push_back(mU32Offsets[i]);
    }
    (*mU64Offsets)[pos] = offset;

    U32Vector(mU32Allocator).swap(mU32Offsets);
}

inline bool AdaptiveAttributeOffsetDumper::IsU64Offset() const
{
    return mU64Offsets != NULL;
}

inline size_t AdaptiveAttributeOffsetDumper::Size() const
{
    if (mU64Offsets)
    {
        return mU64Offsets->size();
    }

    return mU32Offsets.size();
}

inline void AdaptiveAttributeOffsetDumper::Reserve(size_t reserveCount)
{
    mReserveCount = reserveCount;
    if (mU64Offsets)
    {
        mU64Offsets->reserve(reserveCount);
        return;
    }
    mU32Offsets.reserve(reserveCount);
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ADAPTIVE_ATTRIBUTE_OFFSET_DUMPER_H
