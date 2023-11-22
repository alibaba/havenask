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

#include <stddef.h>
#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "indexlib/base/Status.h"
// TODO(xiuchen) change file path
#include "indexlib/util/BlockFpEncoder.h"
#include "indexlib/util/FloatInt8Encoder.h"
#include "indexlib/util/Fp16Encoder.h"

namespace indexlib::config {

class CompressTypeOption
{
public:
    CompressTypeOption();
    ~CompressTypeOption() = default;

public:
    struct CompressType {
        uint16_t uniqEncode    : 1;
        uint16_t equalCompress : 1;
        uint16_t patchCompress : 1;
        uint16_t blockFpEncode : 1;
        uint16_t fp16Encode    : 1;
        uint16_t int8Encode    : 1;
        uint16_t unUsed        : 10;
    };

public:
    Status Init(const std::string& compressType);

    void SetUniqEncode() { _compressType.uniqEncode = 1; }
    void ClearUniqEncode() { _compressType.uniqEncode = 0; }

    bool HasUniqEncodeCompress() const { return _compressType.uniqEncode == 1; }
    bool HasEquivalentCompress() const { return _compressType.equalCompress == 1; }
    bool HasPatchCompress() const { return _compressType.patchCompress == 1; }
    bool HasBlockFpEncodeCompress() const { return _compressType.blockFpEncode == 1; }
    bool HasFp16EncodeCompress() const { return _compressType.fp16Encode == 1; }
    bool HasInt8EncodeCompress() const { return _compressType.int8Encode == 1; }
    bool HasCompressOption() const
    {
        return (HasEquivalentCompress() || HasUniqEncodeCompress() || HasPatchCompress() ||
                HasBlockFpEncodeCompress() || HasFp16EncodeCompress() || HasInt8EncodeCompress());
    }

    float GetInt8AbsMax() const { return _int8Abs; }

    std::string GetCompressStr() const;
    Status AssertEqual(const CompressTypeOption& other) const;
    bool operator==(const CompressTypeOption& other) const;
    bool operator!=(const CompressTypeOption& other) const { return !(*this == other); }
    static const double PATCH_COMPRESS_RATIO;

public:
    static std::string GetCompressStr(bool uniq, bool equal, bool patchCompress, bool blockFpEncode, bool fp16Encode,
                                      bool int8Encode, float int8Abs);

    static inline uint8_t GetSingleValueCompressLen(const CompressTypeOption& compressType)
    {
        if (compressType.HasFp16EncodeCompress()) {
            return sizeof(int16_t);
        }
        if (compressType.HasInt8EncodeCompress()) {
            return sizeof(int8_t);
        }
        return sizeof(float);
    }

    static inline size_t GetMultiValueCompressLen(const CompressTypeOption& compressType, int32_t valueCount)
    {
        // TODO(xiuchen) change file path
        if (compressType.HasBlockFpEncodeCompress()) {
            return indexlib::util::BlockFpEncoder::GetEncodeBytesLen(valueCount);
        }
        if (compressType.HasFp16EncodeCompress()) {
            return indexlib::util::Fp16Encoder::GetEncodeBytesLen(valueCount);
        }
        if (compressType.HasInt8EncodeCompress()) {
            return indexlib::util::FloatInt8Encoder::GetEncodeBytesLen(valueCount);
        }
        return sizeof(float) * valueCount;
    }

public:
    void ClearCompressType()
    {
        _compressType.patchCompress = 0;
        _compressType.uniqEncode = 0;
        _compressType.equalCompress = 0;
        _compressType.blockFpEncode = 0;
        _compressType.fp16Encode = 0;
        _compressType.int8Encode = 0;
        _int8Abs = 0;
    }

private:
    Status AddCompressType(const std::string& type);
    Status Check() const;

private:
    inline static const std::string COMPRESS_UNIQ = "uniq";
    inline static const std::string COMPRESS_EQUAL = "equal";
    inline static const std::string COMPRESS_BLOCKFP = "block_fp";
    inline static const std::string COMPRESS_FP16 = "fp16";
    inline static const std::string COMPRESS_PATCH = "patch_compress";
    inline static const std::string COMPRESS_INT8_PREFIX = "int8#";

private:
    CompressType _compressType;
    float _int8Abs;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::config
