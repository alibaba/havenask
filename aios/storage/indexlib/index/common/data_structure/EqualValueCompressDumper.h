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
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/common/TypedSliceList.h"
#include "indexlib/index/common/data_structure/EqualValueCompressAdvisor.h"

namespace indexlibv2::index {

namespace {
constexpr static uint32_t DEFAULT_SLOT_ITEM_COUNT = 64;
};
template <typename T>
class EqualValueCompressDumper
{
public:
    EqualValueCompressDumper(bool needMagicTail, autil::mem_pool::PoolBase* pool);
    ~EqualValueCompressDumper() {}

public:
    void Reset() { _compressWriter.Init(DEFAULT_SLOT_ITEM_COUNT); }
    void CompressData(T* data, uint32_t count) { _compressWriter.CompressData(data, count); }

    void CompressData(const std::vector<T>& data) { CompressData((T*)data.data(), data.size()); }

    void CompressData(const indexlib::index::TypedSliceList<T>* data, std::vector<int32_t>* newOrder);

    Status Dump(const std::shared_ptr<indexlib::file_system::FileWriter>& file);

private:
    static Status DumpMagicTail(const std::shared_ptr<indexlib::file_system::FileWriter>& file);

private:
    static const uint32_t EQUAL_COMPRESS_SAMPLE_RATIO = 1;

    autil::mem_pool::PoolBase* _pool;
    indexlib::index::EquivalentCompressWriter<T> _compressWriter;
    bool _needMagicTail;

private:
    friend class EqualValueCompressDumperTest;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, EqualValueCompressDumper, T);
/////////////////////////////////////////////////////

template <typename T>
EqualValueCompressDumper<T>::EqualValueCompressDumper(bool needMagicTail, autil::mem_pool::PoolBase* pool)
    : _pool(pool)
    , _compressWriter(pool)
    , _needMagicTail(needMagicTail)
{
    assert(pool);
    _compressWriter.Init(DEFAULT_SLOT_ITEM_COUNT);
}

template <typename T>
inline void EqualValueCompressDumper<T>::CompressData(const indexlib::index::TypedSliceList<T>* data,
                                                      std::vector<int32_t>* newOrder)
{
    // 1MB buffer for compress to control memory use
#define MAX_COMPRESS_DATA_LEN (1 << 20)

    typedef typename autil::mem_pool::pool_allocator<T> AllocatorType;
    AllocatorType allocator(_pool);
    std::vector<T, AllocatorType> dataVec(allocator);
    dataVec.reserve(MAX_COMPRESS_DATA_LEN);

    if (newOrder) {
        uint32_t ite_count = data->Size();
        assert(ite_count == newOrder->size());
        for (uint32_t i = 0; i < ite_count; ++i) {
            T value {};
            data->Read(newOrder->at(i), value);
            dataVec.push_back(value);
            if (dataVec.size() == MAX_COMPRESS_DATA_LEN) {
                _compressWriter.CompressData(dataVec.data(), MAX_COMPRESS_DATA_LEN);
                dataVec.clear();
            }
        }
    } else {
        uint32_t ite_count = data->Size();
        for (uint32_t i = 0; i < ite_count; ++i) {
            T value {};
            data->Read(i, value);
            dataVec.push_back(value);
            if (dataVec.size() == MAX_COMPRESS_DATA_LEN) {
                _compressWriter.CompressData(dataVec.data(), MAX_COMPRESS_DATA_LEN);
                dataVec.clear();
            }
        }
    }
    if (!dataVec.empty()) {
        _compressWriter.CompressData(dataVec.data(), dataVec.size());
    }
}

template <typename T>
inline Status EqualValueCompressDumper<T>::Dump(const std::shared_ptr<indexlib::file_system::FileWriter>& file)
{
    const size_t compressLength = _compressWriter.GetCompressLength();
    uint8_t* buffer = IE_POOL_COMPATIBLE_NEW_VECTOR(_pool, uint8_t, compressLength);
    _compressWriter.DumpBuffer(buffer, compressLength);
    indexlib::index::EquivalentCompressReader<T> reader(buffer);

    if (reader.Size() > 0) {
        auto [status, optSlotItemCount] =
            EqualValueCompressAdvisor<T>::EstimateOptimizeSlotItemCount(reader, EQUAL_COMPRESS_SAMPLE_RATIO);
        RETURN_IF_STATUS_ERROR(status, "estimate optimize slot item count fail for EqualValeCompressDumper");
        _compressWriter.Init(optSlotItemCount);
        status = _compressWriter.CompressData(reader);
        RETURN_IF_STATUS_ERROR(status, "compress data with reader fail");
    }

    size_t reserveSize = _compressWriter.GetCompressLength();
    if (_needMagicTail) {
        reserveSize += sizeof(uint32_t);
    }

    auto st = file->ReserveFile(reserveSize).Status();
    RETURN_IF_STATUS_ERROR(st, "reserve file failed.");

    st = _compressWriter.Dump(file);
    RETURN_IF_STATUS_ERROR(st, "dump compress writer failed.");
    IE_POOL_COMPATIBLE_DELETE_VECTOR(_pool, buffer, compressLength);
    if (_needMagicTail) {
        st = DumpMagicTail(file);
        RETURN_IF_STATUS_ERROR(st, "dump magic tail failed.");
    }
    return Status::OK();
}

template <>
inline Status
EqualValueCompressDumper<uint32_t>::DumpMagicTail(const std::shared_ptr<indexlib::file_system::FileWriter>& file)
{
    uint32_t magicTail = UINT32_OFFSET_TAIL_MAGIC;
    auto [st, _] = file->Write(&magicTail, sizeof(uint32_t)).StatusWith();
    RETURN_IF_STATUS_ERROR(st, "write magic tail failed.");
    return Status::OK();
}

template <>
inline Status
EqualValueCompressDumper<uint64_t>::DumpMagicTail(const std::shared_ptr<indexlib::file_system::FileWriter>& file)
{
    uint32_t magicTail = UINT64_OFFSET_TAIL_MAGIC;
    auto [st, _] = file->Write(&magicTail, sizeof(uint32_t)).StatusWith();
    RETURN_IF_STATUS_ERROR(st, "write magic tail failed.");
    return Status::OK();
}

template <typename T>
inline Status EqualValueCompressDumper<T>::DumpMagicTail(const std::shared_ptr<indexlib::file_system::FileWriter>& file)
{
    // do nothing
    assert(false);
    return Status::OK();
}
} // namespace indexlibv2::index
