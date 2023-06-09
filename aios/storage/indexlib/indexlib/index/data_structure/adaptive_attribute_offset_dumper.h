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
#ifndef __INDEXLIB_ADAPTIVE_ATTRIBUTE_OFFSET_DUMPER_H
#define __INDEXLIB_ADAPTIVE_ATTRIBUTE_OFFSET_DUMPER_H

#include <memory>

#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/common_define.h"
#include "indexlib/index/data_structure/equal_value_compress_dumper.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(file_system, FileWriter);

namespace indexlib { namespace index {

class AdaptiveAttributeOffsetDumper
{
private:
    template <typename T>
    struct OffsetTypes {
        typedef autil::mem_pool::pool_allocator<T> Allocator;
        typedef std::vector<T, Allocator> Vector;
        typedef std::shared_ptr<Vector> VectorPtr;
    };
    typedef OffsetTypes<uint64_t>::Vector U64Vector;
    typedef OffsetTypes<uint32_t>::Vector U32Vector;

public:
    AdaptiveAttributeOffsetDumper(autil::mem_pool::PoolBase* pool);
    ~AdaptiveAttributeOffsetDumper();

public:
    void Init(bool enableAdaptiveOffset, bool useEqualCopmress, uint64_t offsetThreshold);

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
    void WriteOffsetData(const typename OffsetTypes<OffsetType>::Vector& offsetVec, const OffsetFilePtr& offsetFile)
    {
        if (mEnableCompress) {
            EqualValueCompressDumper<OffsetType> compressDumper(true, mPool);
            compressDumper.CompressData((OffsetType*)offsetVec.data(), offsetVec.size());
            compressDumper.Dump(offsetFile);
        } else {
            offsetFile->Write((void*)(&(*(offsetVec.begin()))), sizeof(OffsetType) * offsetVec.size()).GetOrThrow();
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
    if (mU64Offsets) {
        mU64Offsets->push_back(offset);
        return;
    }

    if (offset <= mOffsetThreshold) {
        mU32Offsets.push_back((uint32_t)offset);
        return;
    }

    mU64Offsets.reset(new U64Vector(mU64Allocator));
    if (mReserveCount > 0) {
        mU64Offsets->reserve(mReserveCount);
    }

    for (size_t i = 0; i < mU32Offsets.size(); i++) {
        mU64Offsets->push_back(mU32Offsets[i]);
    }
    mU64Offsets->push_back(offset);

    U32Vector(mU32Allocator).swap(mU32Offsets);
}

inline uint64_t AdaptiveAttributeOffsetDumper::GetOffset(size_t pos) const
{
    assert(pos < Size());

    if (mU64Offsets) {
        return (*mU64Offsets)[pos];
    }
    return mU32Offsets[pos];
}

inline void AdaptiveAttributeOffsetDumper::SetOffset(size_t pos, uint64_t offset)
{
    assert(pos < Size());
    if (mU64Offsets) {
        (*mU64Offsets)[pos] = offset;
        return;
    }

    if (offset <= mOffsetThreshold) {
        mU32Offsets[pos] = (uint32_t)offset;
        return;
    }

    mU64Offsets.reset(new U64Vector(mU64Allocator));
    if (mReserveCount > 0) {
        mU64Offsets->reserve(mReserveCount);
    }

    for (size_t i = 0; i < mU32Offsets.size(); i++) {
        mU64Offsets->push_back(mU32Offsets[i]);
    }
    (*mU64Offsets)[pos] = offset;

    U32Vector(mU32Allocator).swap(mU32Offsets);
}

inline bool AdaptiveAttributeOffsetDumper::IsU64Offset() const { return mU64Offsets != NULL; }

inline size_t AdaptiveAttributeOffsetDumper::Size() const
{
    if (mU64Offsets) {
        return mU64Offsets->size();
    }

    return mU32Offsets.size();
}

inline void AdaptiveAttributeOffsetDumper::Reserve(size_t reserveCount)
{
    mReserveCount = reserveCount;
    if (mU64Offsets) {
        mU64Offsets->reserve(reserveCount);
        return;
    }
    mU32Offsets.reserve(reserveCount);
}
}} // namespace indexlib::index

#endif //__INDEXLIB_ADAPTIVE_ATTRIBUTE_OFFSET_DUMPER_H
