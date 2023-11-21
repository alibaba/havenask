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

#include "indexlib/common/chunk/chunk_define.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace common {

enum ChunkDecoderType {
    ct_plain_integrated = 0,
    ct_plain_slice = 1,
    ct_encoded = 2,
};

class ChunkDecoder
{
public:
    ChunkDecoder(ChunkDecoderType type, uint32_t chunkLen, uint32_t fixedRecordLen)
        : mType(type)
        , mChunkLength(chunkLen)
        , mFixedRecordLen(fixedRecordLen)
    {
    }
    virtual ~ChunkDecoder() {}

public:
    ChunkDecoderType GetType() const { return mType; }

    uint32_t GetChunkLength() const { return mChunkLength; }

protected:
    ChunkDecoderType mType;
    uint32_t mChunkLength;
    uint32_t mFixedRecordLen;
};

DEFINE_SHARED_PTR(ChunkDecoder);
}} // namespace indexlib::common
