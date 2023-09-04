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
#include "indexlib/base/Status.h"
#include "indexlib/index/common/data_structure/EqualValueCompressDumper.h"

namespace indexlib::file_system {
class FileWriter;
}

namespace indexlibv2::index {
class AttributeConfig;

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

    Status Dump(const std::shared_ptr<indexlib::file_system::FileWriter>& offsetFile);

private:
    template <typename OffsetType, typename OffsetFilePtr>
    Status WriteOffsetData(const typename OffsetTypes<OffsetType>::Vector& offsetVec, const OffsetFilePtr& offsetFile)
    {
        if (_enableCompress) {
            EqualValueCompressDumper<OffsetType> compressDumper(true, _pool);
            compressDumper.CompressData((OffsetType*)offsetVec.data(), offsetVec.size());
            auto st = compressDumper.Dump(offsetFile);
            RETURN_IF_STATUS_ERROR(st, "write compress offset failed.");
        } else {
            auto [st, _] =
                offsetFile->Write((void*)(&(*(offsetVec.begin()))), sizeof(OffsetType) * offsetVec.size()).StatusWith();
            RETURN_IF_STATUS_ERROR(st, "write offset failed.");
        }
        return Status::OK();
    }

private:
    autil::mem_pool::PoolBase* _pool;
    bool _enableCompress;
    uint64_t _offsetThreshold;
    uint32_t _reserveCount;

    OffsetTypes<uint32_t>::Allocator _u32Allocator;
    OffsetTypes<uint64_t>::Allocator _u64Allocator;
    U32Vector _u32Offsets;
    OffsetTypes<uint64_t>::VectorPtr _u64Offsets;

private:
    AUTIL_LOG_DECLARE();
};

///////////////////////////////////////////////////////

inline void AdaptiveAttributeOffsetDumper::PushBack(uint64_t offset)
{
    if (_u64Offsets) {
        _u64Offsets->push_back(offset);
        return;
    }

    if (offset <= _offsetThreshold) {
        _u32Offsets.push_back((uint32_t)offset);
        return;
    }

    _u64Offsets.reset(new U64Vector(_u64Allocator));
    if (_reserveCount > 0) {
        _u64Offsets->reserve(_reserveCount);
    }

    for (size_t i = 0; i < _u32Offsets.size(); i++) {
        _u64Offsets->push_back(_u32Offsets[i]);
    }
    _u64Offsets->push_back(offset);

    U32Vector(_u32Allocator).swap(_u32Offsets);
}

inline uint64_t AdaptiveAttributeOffsetDumper::GetOffset(size_t pos) const
{
    assert(pos < Size());

    if (_u64Offsets) {
        return (*_u64Offsets)[pos];
    }
    return _u32Offsets[pos];
}

inline void AdaptiveAttributeOffsetDumper::SetOffset(size_t pos, uint64_t offset)
{
    assert(pos < Size());
    if (_u64Offsets) {
        (*_u64Offsets)[pos] = offset;
        return;
    }

    if (offset <= _offsetThreshold) {
        _u32Offsets[pos] = (uint32_t)offset;
        return;
    }

    _u64Offsets.reset(new U64Vector(_u64Allocator));
    if (_reserveCount > 0) {
        _u64Offsets->reserve(_reserveCount);
    }

    for (size_t i = 0; i < _u32Offsets.size(); i++) {
        _u64Offsets->push_back(_u32Offsets[i]);
    }
    (*_u64Offsets)[pos] = offset;

    U32Vector(_u32Allocator).swap(_u32Offsets);
}

inline bool AdaptiveAttributeOffsetDumper::IsU64Offset() const { return _u64Offsets != NULL; }

inline size_t AdaptiveAttributeOffsetDumper::Size() const
{
    if (_u64Offsets) {
        return _u64Offsets->size();
    }

    return _u32Offsets.size();
}

inline void AdaptiveAttributeOffsetDumper::Reserve(size_t reserveCount)
{
    _reserveCount = reserveCount;
    if (_u64Offsets) {
        _u64Offsets->reserve(reserveCount);
        return;
    }
    _u32Offsets.reserve(reserveCount);
}
} // namespace indexlibv2::index
