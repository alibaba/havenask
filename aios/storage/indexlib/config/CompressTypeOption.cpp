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
#include "indexlib/config/CompressTypeOption.h"

#include <vector>

#include "autil/StringUtil.h"
#include "indexlib/base/Constant.h"

using namespace indexlibv2;

namespace indexlib::config {

AUTIL_LOG_SETUP(indexlib.index, CompressTypeOption);

const double CompressTypeOption::PATCH_COMPRESS_RATIO = 1.0;

CompressTypeOption::CompressTypeOption()
{
    _compressType.uniqEncode = 0;
    _compressType.equalCompress = 0;
    _compressType.patchCompress = 0;
    _compressType.blockFpEncode = 0;
    _compressType.fp16Encode = 0;
    _compressType.int8Encode = 0;
    _compressType.unUsed = 0;
    _int8Abs = 0;
}

Status CompressTypeOption::Init(const std::string& compressType)
{
    std::vector<std::string> types = autil::StringUtil::split(compressType, std::string("|"), true);
    for (size_t i = 0; i < types.size(); i++) {
        autil::StringUtil::trim(types[i]);
        if (!types[i].empty()) {
            auto st = AddCompressType(types[i]);
            RETURN_IF_STATUS_ERROR(st, "add compress type failed, types[%s]", types[i].data());
        }
    }
    return Status::OK();
}

Status CompressTypeOption::AddCompressType(const std::string& type)
{
    if (type == std::string(COMPRESS_UNIQ)) {
        _compressType.uniqEncode = 1;
    } else if (type == std::string(COMPRESS_EQUAL)) {
        _compressType.equalCompress = 1;
    } else if (type == std::string(COMPRESS_PATCH)) {
        _compressType.patchCompress = 1;
    } else if (type == std::string(COMPRESS_BLOCKFP)) {
        _compressType.blockFpEncode = 1;
    } else if (type == std::string(COMPRESS_FP16)) {
        _compressType.fp16Encode = 1;
    } else if (type.find(COMPRESS_INT8_PREFIX) == 0) {
        std::string absStr = type.substr(COMPRESS_INT8_PREFIX.size());
        if (!autil::StringUtil::numberFromString(absStr, _int8Abs)) {
            AUTIL_LOG(ERROR, "bad compress type [%s]", type.data());
            return Status::InvalidArgs("bad compress type");
        }
        _compressType.int8Encode = 1;
    } else {
        AUTIL_LOG(ERROR, "unsupported compress type [%s]", type.data());
        return Status::Unimplement("unsupported compress type");
    }
    return Check();
}

Status CompressTypeOption::Check() const
{
    if (HasInt8EncodeCompress() && _int8Abs <= 0) {
        AUTIL_LOG(ERROR, "invalid abs value[%f] for int8 compress type.", _int8Abs);
        return Status::InvalidArgs("invalid abs value for int8 compress type.");
    }

    int32_t floatCompressCount = 0;
    std::string typeStr;
    if (HasBlockFpEncodeCompress()) {
        floatCompressCount++;
        typeStr += COMPRESS_BLOCKFP + " ";
    }

    if (HasFp16EncodeCompress()) {
        floatCompressCount++;
        typeStr += COMPRESS_FP16 + " ";
    }

    if (HasInt8EncodeCompress()) {
        floatCompressCount++;
        typeStr += COMPRESS_INT8_PREFIX + autil::StringUtil::toString(_int8Abs) + " ";
    }

    if (floatCompressCount > 1) {
        AUTIL_LOG(ERROR, "invalid compress count, types[%s]", typeStr.data());
        return Status::InvalidArgs("invalid compress count");
    }
    return Status::OK();
}

std::string CompressTypeOption::GetCompressStr(bool uniq, bool equal, bool patchCompress, bool blockFpEncode,
                                               bool fp16Encode, bool int8Encode, float int8Abs)
{
    std::string compressStr;
    if (uniq) {
        compressStr += COMPRESS_UNIQ;
        compressStr += "|";
    }

    if (equal) {
        compressStr += COMPRESS_EQUAL;
        compressStr += "|";
    }

    if (patchCompress) {
        compressStr += COMPRESS_PATCH;
        compressStr += "|";
    }

    if (blockFpEncode) {
        compressStr += COMPRESS_BLOCKFP;
        compressStr += "|";
    }

    if (fp16Encode) {
        compressStr += COMPRESS_FP16;
        compressStr += "|";
    }

    if (int8Encode) {
        compressStr += COMPRESS_INT8_PREFIX + autil::StringUtil::toString(int8Abs);
        compressStr += "|";
    }

    if (!compressStr.empty()) {
        compressStr.erase(compressStr.size() - 1, 1);
    }
    return compressStr;
}

std::string CompressTypeOption::GetCompressStr() const
{
    return GetCompressStr(HasUniqEncodeCompress(), HasEquivalentCompress(), HasPatchCompress(),
                          HasBlockFpEncodeCompress(), HasFp16EncodeCompress(), HasInt8EncodeCompress(), _int8Abs);
}

bool CompressTypeOption::operator==(const CompressTypeOption& other) const
{
    return _compressType.uniqEncode == other._compressType.uniqEncode &&
           _compressType.equalCompress == other._compressType.equalCompress &&
           _compressType.patchCompress == other._compressType.patchCompress &&
           _compressType.blockFpEncode == other._compressType.blockFpEncode &&
           _compressType.fp16Encode == other._compressType.fp16Encode &&
           _compressType.int8Encode == other._compressType.int8Encode && _int8Abs == other._int8Abs;
}
Status CompressTypeOption::AssertEqual(const CompressTypeOption& other) const
{
#define CONFIG_ASSERT_EQUAL(a, b, msg)                                                                                 \
    do {                                                                                                               \
        if ((a) != (b)) {                                                                                              \
            RETURN_IF_STATUS_ERROR(Status::ConfigError(), "%s", msg);                                                  \
        }                                                                                                              \
    } while (0)

    CONFIG_ASSERT_EQUAL(_compressType.uniqEncode, other._compressType.uniqEncode, "compress type: uniq not equal");
    CONFIG_ASSERT_EQUAL(_compressType.equalCompress, other._compressType.equalCompress,
                        "compress type: equal not equal");
    CONFIG_ASSERT_EQUAL(_compressType.patchCompress, other._compressType.patchCompress,
                        "compress type: patch compress not equal");
    CONFIG_ASSERT_EQUAL(_compressType.blockFpEncode, other._compressType.blockFpEncode,
                        "compress type: blockFp not equal");
    CONFIG_ASSERT_EQUAL(_compressType.fp16Encode, other._compressType.fp16Encode, "compress type: fp16 not equal");
    CONFIG_ASSERT_EQUAL(_compressType.int8Encode, other._compressType.int8Encode, "compress type: int8 not equal");
    CONFIG_ASSERT_EQUAL(_int8Abs, other._int8Abs, "compress type: int8#[abs_value] not equal");
#undef CONFIG_ASSERT_EQUAL
    return Status::OK();
}

} // namespace indexlib::config
