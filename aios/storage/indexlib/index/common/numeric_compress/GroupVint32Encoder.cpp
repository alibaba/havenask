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
#include "indexlib/index/common/numeric_compress/GroupVint32Encoder.h"

#include "indexlib/index/common/numeric_compress/GroupVarint.h"
#include "indexlib/util/Exception.h"

namespace indexlib::index {
namespace {
}

AUTIL_LOG_SETUP(indexlib.index, GroupVint32Encoder);

GroupVint32Encoder::GroupVint32Encoder() {}

GroupVint32Encoder::~GroupVint32Encoder() {}

std::pair<Status, uint32_t> GroupVint32Encoder::Encode(file_system::ByteSliceWriter& sliceWriter, const uint32_t* src,
                                                       uint32_t srcLen) const
{
    // encode len using int16_t.because of MAX_RECORD_SIZE is 128
    // 4 int32 need max 17 bytes to compress
    uint8_t buffer[ENCODER_BUFFER_SIZE];
    auto [status, encodeLen] = GroupVarint::Compress(buffer, ENCODER_BUFFER_SIZE, src, srcLen);
    RETURN2_IF_STATUS_ERROR(status, 0, "compress fail");

    uint32_t headLen = sizeof(int16_t);

    if (srcLen != 128) {
        sliceWriter.WriteInt16((int16_t)encodeLen | SRC_LEN_FLAG);
        sliceWriter.WriteByte((uint8_t)srcLen);
        headLen = sizeof(int16_t) + sizeof(uint8_t);
    } else {
        sliceWriter.WriteInt16((int16_t)encodeLen);
    }
    sliceWriter.Write(buffer, encodeLen);
    return std::make_pair(Status::OK(), encodeLen + headLen);
}

std::pair<Status, uint32_t> GroupVint32Encoder::Encode(uint8_t* dest, const uint32_t* src, uint32_t srcLen) const
{
    uint32_t destLen = srcLen << 3;
    return GroupVarint::Compress(dest, destLen, src, srcLen);
}

std::pair<Status, uint32_t> GroupVint32Encoder::Decode(uint32_t* dest, uint32_t destLen,
                                                       file_system::ByteSliceReader& sliceReader) const
{
    auto ret = sliceReader.ReadInt16();
    RETURN_RESULT_IF_FS_ERROR(ret.Code(), std::make_pair(ret.Status(), 0), "ReadInt16 failed");

    uint32_t compLen = (uint32_t)ret.Value();
    uint32_t srcLen = 128;

    if (compLen & SRC_LEN_FLAG) {
        auto ret = sliceReader.ReadByte();
        RETURN_RESULT_IF_FS_ERROR(ret.Code(), std::make_pair(ret.Status(), 0), "ReadByte failed");
        srcLen = (uint32_t)ret.Value();
        compLen &= ~SRC_LEN_FLAG;
    }

    uint8_t buffer[ENCODER_BUFFER_SIZE];
    void* bufPtr = buffer;
    auto lenRet = sliceReader.ReadMayCopy(bufPtr, compLen);
    RETURN_RESULT_IF_FS_ERROR(lenRet.Code(), std::make_pair(lenRet.Status(), 0), "ReadMayCopy failed");
    size_t len = lenRet.Value();
    if (len != compLen) {
        RETURN2_IF_STATUS_ERROR(Status::Corruption(), 0, "GroupVarint Decode FAILED.");
    }
    return GroupVarint::Decompress(dest, destLen, (uint8_t*)bufPtr, srcLen);
}

} // namespace indexlib::index
