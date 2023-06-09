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
#include "indexlib/index/data_structure/adaptive_attribute_offset_dumper.h"

#include "indexlib/config/attribute_config.h"
#include "indexlib/index/data_structure/attribute_compress_info.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, AdaptiveAttributeOffsetDumper);

AdaptiveAttributeOffsetDumper::AdaptiveAttributeOffsetDumper(autil::mem_pool::PoolBase* pool)
    : mPool(pool)
    , mEnableCompress(false)
    , mOffsetThreshold(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD)
    , mReserveCount(0)
    , mU32Allocator(pool)
    , mU64Allocator(pool)
    , mU32Offsets(mU32Allocator)
{
}

AdaptiveAttributeOffsetDumper::~AdaptiveAttributeOffsetDumper() {}

void AdaptiveAttributeOffsetDumper::Init(bool enableAdaptiveOffset, bool useEqualCopmress, uint64_t offsetThreshold)
{
    mOffsetThreshold = offsetThreshold;
    mEnableCompress = useEqualCopmress;
    if (!enableAdaptiveOffset) {
        mU64Offsets.reset(new U64Vector(mU64Allocator));
    }
}

void AdaptiveAttributeOffsetDumper::Clear()
{
    U32Vector(mU32Allocator).swap(mU32Offsets);

    if (mU64Offsets) {
        mU64Offsets.reset(new U64Vector(mU64Allocator));
    }

    if (mReserveCount > 0) {
        Reserve(mReserveCount);
    }
}

void AdaptiveAttributeOffsetDumper::Dump(const FileWriterPtr& offsetFile)
{
    if (!mEnableCompress) {
        size_t offsetUnitSize = IsU64Offset() ? sizeof(uint64_t) : sizeof(uint32_t);
        offsetFile->ReserveFile(offsetUnitSize * Size()).GetOrThrow();
    }

    if (mU64Offsets) {
        WriteOffsetData<uint64_t, FileWriterPtr>(*mU64Offsets, offsetFile);
    } else {
        WriteOffsetData<uint32_t, FileWriterPtr>(mU32Offsets, offsetFile);
    }
}
}} // namespace indexlib::index
