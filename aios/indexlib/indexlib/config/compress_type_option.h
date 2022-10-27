#ifndef __INDEXLIB_COMPRESS_TYPE_OPTION_H
#define __INDEXLIB_COMPRESS_TYPE_OPTION_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/block_fp_encoder.h"
#include "indexlib/util/fp16_encoder.h"
#include "indexlib/util/float_int8_encoder.h"

IE_NAMESPACE_BEGIN(config);

class CompressTypeOption
{
public:
    CompressTypeOption();
    ~CompressTypeOption();
public:
    struct CompressType
    {
        uint16_t uniqEncode : 1;
        uint16_t equalCompress : 1;
        uint16_t patchCompress : 1;
        uint16_t blockFpEncode : 1;
        uint16_t fp16Encode : 1;
        uint16_t int8Encode : 1;
        uint16_t unUsed : 10;
    };

public:
    void SetUniqEncode() { mCompressType.uniqEncode = 1; }
    void ClearUniqEncode() { mCompressType.uniqEncode = 0; }
    void Init(const std::string& compressType);
    bool HasUniqEncodeCompress() const
    { return mCompressType.uniqEncode == 1; }

    bool HasEquivalentCompress() const
    { return mCompressType.equalCompress == 1; }

    bool HasPatchCompress() const 
    { return mCompressType.patchCompress == 1; }

    bool HasBlockFpEncodeCompress() const
    { return mCompressType.blockFpEncode == 1; }

    bool HasFp16EncodeCompress() const
    { return mCompressType.fp16Encode == 1; }

    bool HasInt8EncodeCompress() const
    { return mCompressType.int8Encode == 1; }

    bool HasCompressOption() const
    {
        return (HasEquivalentCompress() || HasUniqEncodeCompress()
                || HasPatchCompress() || HasBlockFpEncodeCompress()
                || HasFp16EncodeCompress() || HasInt8EncodeCompress());
    }

    float GetInt8AbsMax() const { return mInt8Abs; }
    
    std::string GetCompressStr() const;
    void AssertEqual(const CompressTypeOption& other) const;
    bool operator == (const CompressTypeOption& other) const;
    bool operator != (const CompressTypeOption& other) const
    {
        return !(*this == other);
    }
    static const double PATCH_COMPRESS_RATIO;
    
public:
    static std::string GetCompressStr(
            bool uniq, bool equal, bool patchCompress,
            bool blockFpEncode = false, bool fp16Encode = false,
            bool int8Encode = false, float int8Abs = 0.0f);

public:
    //for test
    void ClearCompressType()
    {
        mCompressType.patchCompress = 0;
        mCompressType.uniqEncode = 0;
        mCompressType.equalCompress = 0;
        mCompressType.blockFpEncode = 0;
        mCompressType.fp16Encode = 0;
        mCompressType.int8Encode = 0;
        mInt8Abs = 0;
    }

public:
    static inline uint8_t GetSingleValueCompressLen(
        const config::CompressTypeOption& compressType)
    {
        if (compressType.HasFp16EncodeCompress())
        {
            return sizeof(int16_t);
        }
        if (compressType.HasInt8EncodeCompress())
        {
            return sizeof(int8_t);
        }
        return sizeof(float);    
    }
    static inline size_t GetMultiValueCompressLen(
        const config::CompressTypeOption& compressType, int32_t valueCount)
    {
        if (compressType.HasBlockFpEncodeCompress())
        {
            return util::BlockFpEncoder::GetEncodeBytesLen(valueCount);
        }
        if (compressType.HasFp16EncodeCompress())
        {
            return util::Fp16Encoder::GetEncodeBytesLen(valueCount);
        }
        if (compressType.HasInt8EncodeCompress())
        {
            return util::FloatInt8Encoder::GetEncodeBytesLen(valueCount);
        }
        return sizeof(float) * valueCount;
    }
private:
    void AddCompressType(const std::string& type);
    void Check() const;
private:
    CompressType mCompressType;
    float mInt8Abs;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CompressTypeOption);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_COMPRESS_TYPE_OPTION_H
