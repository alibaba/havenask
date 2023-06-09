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

#include "indexlib/base/Status.h"
#include "indexlib/util/Exception.h"

namespace indexlib::index {

class VByteCompressor
{
public:
    /**
     * encode one int32 value to buffer
     * @return length after encode
     */
    static std::pair<Status, uint32_t> EncodeVInt32(uint8_t* outputByte, uint32_t maxOutputByte, int32_t value);

    /**
     * decode one vint32 to int32
     *
     * @param inputByte input byte buffer, and will be increased by the number of bytes have been decoded
     * @param inputByte size of input byte buffer, and will be decreased by the number of bytes have been decoded.
     * @return int32 value
     */
    static std::pair<Status, int32_t> DecodeVInt32(uint8_t*& inputByte, uint32_t& inputByteLength);

    /**
     * Get the vint32 length of int32
     */
    static uint32_t GetVInt32Length(int32_t value);

    /* encode one uint32 value to buffer pointed by cursor */
    static void WriteVUInt32(uint32_t value, char*& cursor);

    static uint32_t ReadVUInt32(char*& cursor);

private:
    static std::pair<Status, uint8_t> ReadByte(const uint8_t* inputByte, uint32_t& pos, uint32_t maxInputByte);
    static Status WriteByte(uint8_t* outputByte, uint32_t& pos, uint32_t maxOutputByte, uint8_t value);

private:
    AUTIL_LOG_DECLARE();
};

/////////////////////////////////////////////////////
//
inline std::pair<Status, uint8_t> VByteCompressor::ReadByte(const uint8_t* inputByte, uint32_t& pos,
                                                            uint32_t maxInputByte)
{
    if (pos >= maxInputByte) {
        RETURN2_IF_STATUS_ERROR(Status::OutOfRange(), 0, "Buffer is overflow");
    }
    return std::make_pair(Status::OK(), inputByte[pos++]);
}

inline Status VByteCompressor::WriteByte(uint8_t* outputByte, uint32_t& pos, uint32_t maxOutputByte, uint8_t value)
{
    if (pos >= maxOutputByte) {
        RETURN_IF_STATUS_ERROR(Status::OutOfRange(), "Buffer is overflow");
    }
    outputByte[pos++] = value;
    return Status::OK();
}

inline uint32_t VByteCompressor::GetVInt32Length(int32_t value)
{
    uint8_t l = 1;
    uint32_t ui = value;
    while ((ui & ~0x7F) != 0) {
        l++;
        ui >>= 7; // doing unsigned shift
    }
    return l;
}

inline void VByteCompressor::WriteVUInt32(uint32_t value, char*& cursor)
{
    while (value > 0x7F) {
        *cursor++ = 0x80 | (value & 0x7F);
        value >>= 7;
    }
    *cursor++ = value & 0x7F;
}

inline uint32_t VByteCompressor::ReadVUInt32(char*& cursor)
{
    uint8_t byte = *(uint8_t*)cursor++;
    uint32_t value = byte & 0x7F;
    int shift = 7;

    while (byte & 0x80) {
        byte = *(uint8_t*)cursor++;
        value |= ((byte & 0x7F) << shift);
        shift += 7;
    }
    return value;
}

inline std::pair<Status, uint32_t> VByteCompressor::EncodeVInt32(uint8_t* outputByte, uint32_t maxOutputByte,
                                                                 int32_t value)
{
    uint32_t l = 0;
    uint32_t ui = value;
    while ((ui & ~0x7F) != 0) {
        auto status = WriteByte(outputByte, l, maxOutputByte, (uint8_t)((ui & 0x7f) | 0x80));
        RETURN2_IF_STATUS_ERROR(status, 0, "write byte fail");
        ui >>= 7;
    }
    auto status = WriteByte(outputByte, l, maxOutputByte, (uint8_t)ui);
    return std::make_pair(status, l);
}

inline std::pair<Status, int32_t> VByteCompressor::DecodeVInt32(uint8_t*& inputByte, uint32_t& inputByteLength)
{
    uint32_t l = 0;
    auto [status, b] = ReadByte(inputByte, l, inputByteLength);
    RETURN2_IF_STATUS_ERROR(status, 0, "read byte fail");
    uint32_t i = b & 0x7F;
    for (int32_t shift = 7; (b & 0x80) != 0; shift += 7) {
        auto statusWith = ReadByte(inputByte, l, inputByteLength);
        RETURN2_IF_STATUS_ERROR(statusWith.first, 0, "read byte fail");
        b = statusWith.second;
        i |= (b & 0x7FL) << shift;
    }
    inputByte += l;
    inputByteLength -= l;
    return std::make_pair(Status::OK(), i);
}
} // namespace indexlib::index
