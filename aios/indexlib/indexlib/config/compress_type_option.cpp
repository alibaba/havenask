#include "indexlib/config/compress_type_option.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/config_define.h"
#include "autil/StringUtil.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, CompressTypeOption);

const double CompressTypeOption::PATCH_COMPRESS_RATIO = 1.0;

CompressTypeOption::CompressTypeOption() 
{
    mCompressType.uniqEncode = 0;
    mCompressType.equalCompress = 0;
    mCompressType.patchCompress = 0;
    mCompressType.blockFpEncode = 0;
    mCompressType.fp16Encode = 0;
    mCompressType.int8Encode = 0;
    mCompressType.unUsed = 0;
    mInt8Abs = 0;
}

CompressTypeOption::~CompressTypeOption() 
{
}

void CompressTypeOption::Init(const string& compressType)
{
    vector<string> types = StringUtil::split(compressType, string("|"), true);
    for (size_t i = 0; i < types.size(); i++)
    {
        StringUtil::trim(types[i]);
        if (!types[i].empty())
        {
            AddCompressType(types[i]);
        }
    }
}

void CompressTypeOption::AddCompressType(const string& type)
{
    if (type == string(COMPRESS_UNIQ))
    {
        mCompressType.uniqEncode = 1;
    }
    else if (type == string(COMPRESS_EQUAL))
    {
        mCompressType.equalCompress = 1;
    }
    else if (type == string(COMPRESS_PATCH))
    {
        mCompressType.patchCompress = 1;
    }
    else if (type == string(COMPRESS_BLOCKFP)) {
        mCompressType.blockFpEncode = 1;
    }
    else if (type == string(COMPRESS_FP16)) {
        mCompressType.fp16Encode = 1;
    }
    else if (type.find(COMPRESS_INT8_PREFIX) == 0) {
        string absStr = type.substr(COMPRESS_INT8_PREFIX.size());
        if (!StringUtil::numberFromString(absStr, mInt8Abs))
        {
            INDEXLIB_FATAL_ERROR(BadParameter, "bad compress type [%s]", type.c_str());
        }
        mCompressType.int8Encode = 1;
    }
    else
    {
        INDEXLIB_FATAL_ERROR(Schema, "unsupported compress type:%s", type.c_str());
    }
    Check();
}

void CompressTypeOption::Check() const
{
    if (HasInt8EncodeCompress() && mInt8Abs <= 0)
    {
        INDEXLIB_FATAL_ERROR(BadParameter,
                       "int8 compress type with invalid abs value [%f]", mInt8Abs);
    }
    
    int32_t floatCompressCount = 0;
    string typeStr;
    if (HasBlockFpEncodeCompress())
    {
        floatCompressCount++;
        typeStr += COMPRESS_BLOCKFP + " ";
    }
    
    if (HasFp16EncodeCompress()) {
        floatCompressCount++;
        typeStr += COMPRESS_FP16 + " ";
    }

    if (HasInt8EncodeCompress()) {
        floatCompressCount++;
        typeStr += COMPRESS_INT8_PREFIX + StringUtil::toString(mInt8Abs) + " ";
    }

    if (floatCompressCount > 1)
    {
        INDEXLIB_FATAL_ERROR(Schema, "%s at the same time", typeStr.c_str());
    }

}

string CompressTypeOption::GetCompressStr(bool uniq, bool equal, bool patchCompress,
        bool blockFpEncode, bool fp16Encode, bool int8Encode, float int8Abs)
{
    string compressStr;
    if (uniq)
    {
        compressStr += COMPRESS_UNIQ;
        compressStr += "|";
    }

    if (equal)
    {
        compressStr += COMPRESS_EQUAL;
        compressStr += "|";
    }

    if (patchCompress)
    {
        compressStr += COMPRESS_PATCH;
        compressStr += "|";
    }

    if (blockFpEncode)
    {
        compressStr += COMPRESS_BLOCKFP;
        compressStr += "|";
    }

    if (fp16Encode)
    {
        compressStr += COMPRESS_FP16;
        compressStr += "|";
    }

    if (int8Encode)
    {
        compressStr += COMPRESS_INT8_PREFIX + StringUtil::toString(int8Abs); 
        compressStr += "|";       
    }

    if (!compressStr.empty())
    {
        compressStr.erase(compressStr.size() - 1, 1);
    }
    return compressStr;

}

string CompressTypeOption::GetCompressStr() const
{
    return GetCompressStr(HasUniqEncodeCompress(), 
                          HasEquivalentCompress(),
                          HasPatchCompress(),
                          HasBlockFpEncodeCompress(),
                          HasFp16EncodeCompress(),
                          HasInt8EncodeCompress(),
                          mInt8Abs);
}

void CompressTypeOption::AssertEqual(const CompressTypeOption& other) const
{
    IE_CONFIG_ASSERT_EQUAL(mCompressType.uniqEncode, other.mCompressType.uniqEncode, "compress type: uniq not equal");
    IE_CONFIG_ASSERT_EQUAL(mCompressType.equalCompress, other.mCompressType.equalCompress, "compress type: equal not equal");
    IE_CONFIG_ASSERT_EQUAL(mCompressType.patchCompress, other.mCompressType.patchCompress, "compress type: patch compress not equal");
    IE_CONFIG_ASSERT_EQUAL(mCompressType.blockFpEncode, other.mCompressType.blockFpEncode, "compress type: blockFp not equal");
    IE_CONFIG_ASSERT_EQUAL(mCompressType.fp16Encode, other.mCompressType.fp16Encode, "compress type: fp16 not equal");
    IE_CONFIG_ASSERT_EQUAL(mCompressType.int8Encode, other.mCompressType.int8Encode, "compress type: int8 not equal");
    IE_CONFIG_ASSERT_EQUAL(mInt8Abs, other.mInt8Abs, "compress type: int8#[abs_value] not equal");
}

bool CompressTypeOption::operator == (
        const CompressTypeOption& other) const
{
    return mCompressType.uniqEncode == other.mCompressType.uniqEncode
        && mCompressType.equalCompress == other.mCompressType.equalCompress
        && mCompressType.patchCompress == other.mCompressType.patchCompress
        && mCompressType.blockFpEncode == other.mCompressType.blockFpEncode
        && mCompressType.fp16Encode == other.mCompressType.fp16Encode
        && mCompressType.int8Encode == other.mCompressType.int8Encode
        && mInt8Abs == other.mInt8Abs;
}

IE_NAMESPACE_END(config);

