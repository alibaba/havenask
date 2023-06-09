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
#include "indexlib/index/common/data_structure/AdaptiveAttributeOffsetDumper.h"

#include "indexlib/index/common/data_structure/AttributeCompressInfo.h"

using namespace indexlib::config;
using namespace indexlib::file_system;

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, AdaptiveAttributeOffsetDumper);

AdaptiveAttributeOffsetDumper::AdaptiveAttributeOffsetDumper(autil::mem_pool::PoolBase* pool)
    : _pool(pool)
    , _enableCompress(false)
    , _offsetThreshold(indexlib::index::VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD)
    , _reserveCount(0)
    , _u32Allocator(pool)
    , _u64Allocator(pool)
    , _u32Offsets(_u32Allocator)
{
}

AdaptiveAttributeOffsetDumper::~AdaptiveAttributeOffsetDumper() {}

void AdaptiveAttributeOffsetDumper::Init(bool enableAdaptiveOffset, bool useEqualCopmress, uint64_t offsetThreshold)
{
    _offsetThreshold = offsetThreshold;
    _enableCompress = useEqualCopmress;
    if (!enableAdaptiveOffset) {
        _u64Offsets.reset(new U64Vector(_u64Allocator));
    }
}

void AdaptiveAttributeOffsetDumper::Clear()
{
    U32Vector(_u32Allocator).swap(_u32Offsets);

    if (_u64Offsets) {
        _u64Offsets.reset(new U64Vector(_u64Allocator));
    }

    if (_reserveCount > 0) {
        Reserve(_reserveCount);
    }
}

Status AdaptiveAttributeOffsetDumper::Dump(const std::shared_ptr<indexlib::file_system::FileWriter>& offsetFile)
{
    auto st = Status::OK();
    if (!_enableCompress) {
        size_t offsetUnitSize = IsU64Offset() ? sizeof(uint64_t) : sizeof(uint32_t);
        st = offsetFile->ReserveFile(offsetUnitSize * Size()).Status();
        RETURN_IF_STATUS_ERROR(st, "reserve offset file failed.");
    }

    if (_u64Offsets) {
        st = WriteOffsetData<uint64_t, std::shared_ptr<indexlib::file_system::FileWriter>>(*_u64Offsets, offsetFile);
    } else {
        st = WriteOffsetData<uint32_t, std::shared_ptr<indexlib::file_system::FileWriter>>(_u32Offsets, offsetFile);
    }
    RETURN_IF_STATUS_ERROR(st, "write offset data failed.");
    return Status::OK();
}
} // namespace indexlibv2::index
