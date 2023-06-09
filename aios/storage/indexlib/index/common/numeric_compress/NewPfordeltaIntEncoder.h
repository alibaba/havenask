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

#include "indexlib/index/common/numeric_compress/IntEncoder.h"
#include "indexlib/index/common/numeric_compress/NewPfordeltaCompressor.h"
#include "indexlib/index/common/numeric_compress/NosseNewPfordeltaCompressor.h"
#include "indexlib/util/Exception.h"

namespace indexlib::index {

template <typename T, typename Compressor>
class NewPForDeltaIntEncoder : public IntEncoder<T>
{
public:
    const static size_t ENCODER_BUFFER_SIZE = MAX_RECORD_SIZE + MAX_RECORD_SIZE / 2;
    const static size_t ENCODER_BUFFER_BYTE_SIZE = ENCODER_BUFFER_SIZE * sizeof(uint32_t);

public:
    NewPForDeltaIntEncoder(bool enableBlockOpt = false) : _enableBlockOpt(enableBlockOpt) {}

    ~NewPForDeltaIntEncoder() {}

public:
    std::pair<Status, uint32_t> Encode(indexlib::file_system::ByteSliceWriter& sliceWriter, const T* src,
                                       uint32_t srcLen) const override;
    std::pair<Status, uint32_t> Encode(uint8_t* dest, const T* src, uint32_t srcLen) const override;
    std::pair<Status, uint32_t> Decode(T* dest, uint32_t destLen,
                                       indexlib::file_system::ByteSliceReader& sliceReader) const override;

    // size_t GetCompressedLength(uint32_t head)
    // {
    //     return _compressor.GetCompressedLength(head) * sizeof(uint32_t);
    // }
private:
    Compressor _compressor;
    bool _enableBlockOpt;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE_2(indexlib.index, NewPForDeltaIntEncoder, T, Compressor);

template <typename T, typename Compressor>
std::pair<Status, uint32_t>
NewPForDeltaIntEncoder<T, Compressor>::Encode(indexlib::file_system::ByteSliceWriter& sliceWriter, const T* src,
                                              uint32_t srcLen) const
{
    uint8_t buffer[ENCODER_BUFFER_BYTE_SIZE];
    auto [status, encodeLen] = Encode(buffer, src, srcLen);
    RETURN2_IF_STATUS_ERROR(status, 0, "encode fail");
    sliceWriter.Write((const uint8_t*)buffer, encodeLen);
    return std::make_pair(Status::OK(), encodeLen);
}

template <typename T, typename Compressor>
std::pair<Status, uint32_t> NewPForDeltaIntEncoder<T, Compressor>::Encode(uint8_t* dest, const T* src,
                                                                          uint32_t srcLen) const
{
    auto [status, len] = _compressor.Compress((uint32_t*)dest, ENCODER_BUFFER_SIZE, src, srcLen, _enableBlockOpt);
    RETURN2_IF_STATUS_ERROR(status, 0, "compress fail");
    return std::make_pair(Status::OK(), len * sizeof(uint32_t));
}

template <typename T, typename Compressor>
std::pair<Status, uint32_t>
NewPForDeltaIntEncoder<T, Compressor>::Decode(T* dest, uint32_t destLen,
                                              indexlib::file_system::ByteSliceReader& sliceReader) const
{
    uint8_t buffer[ENCODER_BUFFER_BYTE_SIZE];
    uint32_t header = (uint32_t)sliceReader.PeekInt32();
    size_t compLen = _compressor.GetCompressedLength(header) * sizeof(uint32_t);
    assert(compLen <= ENCODER_BUFFER_BYTE_SIZE);
    void* bufPtr = buffer;
    size_t len = sliceReader.ReadMayCopy(bufPtr, compLen);
    if (len != compLen) {
        RETURN2_IF_STATUS_ERROR(Status::Corruption(), 0, "Decode posting FAILED.");
    }
    return std::make_pair(Status::OK(),
                          (uint32_t)_compressor.Decompress(dest, destLen, (const uint32_t*)bufPtr, compLen));
}
#define NEW_PFORDELTA_INTENCODER(type)                                                                                 \
    typedef NewPForDeltaIntEncoder<uint##type##_t, NewPForDeltaCompressor> NewPForDeltaInt##type##Encoder;

NEW_PFORDELTA_INTENCODER(8);
NEW_PFORDELTA_INTENCODER(16);
NEW_PFORDELTA_INTENCODER(32);

#define NOSSE_NEW_PFORDELTA_INTENCODER(type)                                                                           \
    typedef NewPForDeltaIntEncoder<uint##type##_t, NosseNewPForDeltaCompressor> NoSseNewPForDeltaInt##type##Encoder;

NOSSE_NEW_PFORDELTA_INTENCODER(32);
} // namespace indexlib::index
