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
#ifndef __INDEXLIB_INTEGRATED_PLAIN_CHUNK_DECODER_H
#define __INDEXLIB_INTEGRATED_PLAIN_CHUNK_DECODER_H

#include <memory>

#include "autil/ConstString.h"
#include "autil/MultiValueType.h"
#include "indexlib/common/chunk/chunk_decoder.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace common {

class IntegratedPlainChunkDecoder : public ChunkDecoder
{
public:
    IntegratedPlainChunkDecoder(const char* baseAddress, uint32_t chunkLen, uint32_t fixedRecordLen)
        : ChunkDecoder(ct_plain_integrated, chunkLen, fixedRecordLen)
        , mCursor(0)
        , mBaseAddress(baseAddress)
    {
    }

    ~IntegratedPlainChunkDecoder() {}

public:
    void Seek(uint32_t position) { mCursor = position; }

    int64_t Read(autil::StringView& value, uint32_t len)
    {
        value = autil::StringView(mBaseAddress + mCursor, len);
        mCursor += len;
        assert(mCursor <= mChunkLength);
        return len;
    }

    int64_t ReadRecord(autil::StringView& value)
    {
        int64_t readSize = 0;
        if (mFixedRecordLen == 0) {
            autil::MultiChar multiChar;
            multiChar.init(mBaseAddress + mCursor);
            value = autil::StringView(multiChar.data(), multiChar.size());
            auto length = multiChar.length();
            mCursor += length;
            readSize = length;
        } else {
            value = autil::StringView(mBaseAddress + mCursor, mFixedRecordLen);
            mCursor += mFixedRecordLen;
            readSize = mFixedRecordLen;
        }
        return readSize;
    }

    bool IsEnd() const { return mCursor == mChunkLength; }

private:
    uint32_t mCursor;
    const char* mBaseAddress;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IntegratedPlainChunkDecoder);
}} // namespace indexlib::common

#endif //__INDEXLIB_INTEGRATED_PLAIN_CHUNK_DECODER_H
