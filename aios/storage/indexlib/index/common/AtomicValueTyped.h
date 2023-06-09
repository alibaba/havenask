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

#include "indexlib/index/common/AtomicValue.h"
#include "indexlib/index/common/numeric_compress/EncoderProvider.h"
#include "indexlib/util/Status2Exception.h"

namespace indexlib::index {

template <typename T>
struct ValueTypeTraits {
    static const AtomicValue::ValueType TYPE = AtomicValue::vt_unknown;
};

#define FORMAT_TYPE_TRAITS_HELPER(x, y)                                                                                \
    template <>                                                                                                        \
    struct ValueTypeTraits<x> {                                                                                        \
        static const AtomicValue::ValueType TYPE = y;                                                                  \
    }

FORMAT_TYPE_TRAITS_HELPER(int8_t, AtomicValue::vt_int8);
FORMAT_TYPE_TRAITS_HELPER(int16_t, AtomicValue::vt_int16);
FORMAT_TYPE_TRAITS_HELPER(int32_t, AtomicValue::vt_int32);
FORMAT_TYPE_TRAITS_HELPER(uint8_t, AtomicValue::vt_uint8);
FORMAT_TYPE_TRAITS_HELPER(uint16_t, AtomicValue::vt_uint16);
FORMAT_TYPE_TRAITS_HELPER(uint32_t, AtomicValue::vt_uint32);

template <typename T>
struct EncoderTypeTraits {
    struct UnknownType {
    };
    typedef UnknownType Encoder;
};

#define ENCODER_TYPE_TRAITS_HELPER(type, encoder, data_type)                                                           \
    template <>                                                                                                        \
    struct EncoderTypeTraits<type> {                                                                                   \
        typedef encoder Encoder;                                                                                       \
        typedef data_type DataType;                                                                                    \
    };

ENCODER_TYPE_TRAITS_HELPER(int8_t, Int8Encoder, uint8_t);
ENCODER_TYPE_TRAITS_HELPER(uint8_t, Int8Encoder, uint8_t);
ENCODER_TYPE_TRAITS_HELPER(int16_t, Int16Encoder, uint16_t);
ENCODER_TYPE_TRAITS_HELPER(uint16_t, Int16Encoder, uint16_t);
ENCODER_TYPE_TRAITS_HELPER(int32_t, Int32Encoder, uint32_t);
ENCODER_TYPE_TRAITS_HELPER(uint32_t, Int32Encoder, uint32_t);

template <typename T>
class AtomicValueTyped : public AtomicValue
{
public:
    typedef typename EncoderTypeTraits<T>::Encoder Encoder;
    typedef typename EncoderTypeTraits<T>::DataType DataType;

public:
    void SetValue(T value) { _value = value; }
    T GetValue() const { return _value; }

    ValueType GetType() const override { return ValueTypeTraits<T>::TYPE; }

    size_t GetSize() const override { return sizeof(T); }

    void SetEncoder(uint8_t mode, const Encoder* encoder)
    {
        assert(mode < 4);
        _encoders[mode] = encoder;
    }

    const Encoder* GetEncoder(uint8_t mode) { return (mode < 4) ? _encoders[mode] : nullptr; }

    uint32_t Encode(uint8_t mode, file_system::ByteSliceWriter& sliceWriter, const uint8_t* src,
                    uint32_t len) const override
    {
        assert(mode < 4);
        assert(_encoders[mode]);
        auto [status, destLen] = _encoders[mode]->Encode(sliceWriter, (const DataType*)src, len / sizeof(T));
        THROW_IF_STATUS_ERROR(status);
        return destLen;
    }

    uint32_t Decode(uint8_t mode, uint8_t* dest, uint32_t destLen,
                    file_system::ByteSliceReader& sliceReader) const override
    {
        assert(mode < 4);
        assert(_encoders[mode]);
        auto [status, len] = _encoders[mode]->Decode((DataType*)dest, destLen / sizeof(T), sliceReader);
        THROW_IF_STATUS_ERROR(status);
        return len;
    }

private:
    T _value;
    // TODO: use CompressMode in index_define.h
    // TODO: set encoder pointer NULL
    const Encoder* _encoders[4];

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
