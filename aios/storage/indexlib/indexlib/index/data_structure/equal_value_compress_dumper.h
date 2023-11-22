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

#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/common_define.h"
#include "indexlib/index/common/TypedSliceList.h"
#include "indexlib/index/data_structure/equal_value_compress_advisor.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Status2Exception.h"

namespace indexlib { namespace index {

template <typename T>
class EqualValueCompressDumper
{
public:
    EqualValueCompressDumper(bool needMagicTail, autil::mem_pool::PoolBase* pool);
    ~EqualValueCompressDumper() {}

public:
    void Reset() { mCompressWriter.Init(INIT_SLOT_ITEM_COUNT); }
    void CompressData(T* data, uint32_t count) { mCompressWriter.CompressData(data, count); }

    void CompressData(const std::vector<T>& data) { CompressData((T*)data.data(), data.size()); }

    void CompressData(const TypedSliceList<T>* data, std::vector<int32_t>* newOrder);

    void Dump(const file_system::FileWriterPtr& file);

private:
    static void DumpMagicTail(const file_system::FileWriterPtr& file);

private:
    static const uint32_t EQUAL_COMPRESS_SAMPLE_RATIO = 1;

    autil::mem_pool::PoolBase* mPool;
    EquivalentCompressWriter<T> mCompressWriter;
    bool mNeedMagicTail;

private:
    friend class EqualValueCompressDumperTest;
};

/////////////////////////////////////////////////////

template <typename T>
EqualValueCompressDumper<T>::EqualValueCompressDumper(bool needMagicTail, autil::mem_pool::PoolBase* pool)
    : mPool(pool)
    , mCompressWriter(pool)
    , mNeedMagicTail(needMagicTail)
{
    assert(pool);
    mCompressWriter.Init(INIT_SLOT_ITEM_COUNT);
}

template <typename T>
inline void EqualValueCompressDumper<T>::CompressData(const TypedSliceList<T>* data, std::vector<int32_t>* newOrder)
{
    // 1MB buffer for compress to control memory use
#define MAX_COMPRESS_DATA_LEN (1 << 20)

    typedef typename autil::mem_pool::pool_allocator<T> AllocatorType;
    AllocatorType allocator(mPool);
    std::vector<T, AllocatorType> dataVec(allocator);
    dataVec.reserve(MAX_COMPRESS_DATA_LEN);

    if (newOrder) {
        uint32_t itemCount = data->Size();
        assert(itemCount == newOrder->size());
        for (uint32_t i = 0; i < itemCount; ++i) {
            T value {};
            data->Read(newOrder->at(i), value);
            dataVec.push_back(value);
            if (dataVec.size() == MAX_COMPRESS_DATA_LEN) {
                mCompressWriter.CompressData(dataVec.data(), MAX_COMPRESS_DATA_LEN);
                dataVec.clear();
            }
        }
    } else {
        uint32_t itemCount = data->Size();
        for (uint32_t i = 0; i < itemCount; ++i) {
            T value {};
            data->Read(i, value);
            dataVec.push_back(value);
            if (dataVec.size() == MAX_COMPRESS_DATA_LEN) {
                mCompressWriter.CompressData(dataVec.data(), MAX_COMPRESS_DATA_LEN);
                dataVec.clear();
            }
        }
    }
    if (!dataVec.empty()) {
        mCompressWriter.CompressData(dataVec.data(), dataVec.size());
    }
}

template <typename T>
inline void EqualValueCompressDumper<T>::Dump(const file_system::FileWriterPtr& file)
{
    const size_t compressLength = mCompressWriter.GetCompressLength();
    uint8_t* buffer = IE_POOL_COMPATIBLE_NEW_VECTOR(mPool, uint8_t, compressLength);
    mCompressWriter.DumpBuffer(buffer, compressLength);
    EquivalentCompressReader<T> reader(buffer);

    if (reader.Size() > 0) {
        uint32_t optSlotItemCount =
            EqualValueCompressAdvisor<T>::EstimateOptimizeSlotItemCount(reader, EQUAL_COMPRESS_SAMPLE_RATIO);

        mCompressWriter.Init(optSlotItemCount);
        indexlib::util::ThrowIfStatusError(mCompressWriter.CompressData(reader));
    }

    size_t reserveSize = mCompressWriter.GetCompressLength();
    if (mNeedMagicTail) {
        reserveSize += sizeof(uint32_t);
    }

    file->ReserveFile(reserveSize).GetOrThrow();

    indexlib::util::ThrowIfStatusError(mCompressWriter.Dump(file));
    IE_POOL_COMPATIBLE_DELETE_VECTOR(mPool, buffer, compressLength);
    if (mNeedMagicTail) {
        DumpMagicTail(file);
    }
}

template <>
inline void EqualValueCompressDumper<uint32_t>::DumpMagicTail(const file_system::FileWriterPtr& file)
{
    uint32_t magicTail = UINT32_OFFSET_TAIL_MAGIC;
    file->Write(&magicTail, sizeof(uint32_t)).GetOrThrow();
}

template <>
inline void EqualValueCompressDumper<uint64_t>::DumpMagicTail(const file_system::FileWriterPtr& file)
{
    uint32_t magicTail = UINT64_OFFSET_TAIL_MAGIC;
    file->Write(&magicTail, sizeof(uint32_t)).GetOrThrow();
}

template <typename T>
inline void EqualValueCompressDumper<T>::DumpMagicTail(const file_system::FileWriterPtr& file)
{
    // do nothing
    assert(false);
}
}} // namespace indexlib::index
