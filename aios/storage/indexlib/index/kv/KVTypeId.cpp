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
#include "indexlib/index/kv/KVTypeId.h"

#include "autil/Log.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/FieldTypeTraits.h"
#include "indexlib/index/kv/KVFormatOptions.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/KVIndexPreference.h"
#include "indexlib/index/kv/config/ValueConfig.h"

namespace indexlibv2::index {
AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.index, KVTypeId);

static const char* PrintBool(bool flag) { return flag ? "true" : "false"; }

KVTypeId::KVTypeId(KVIndexType onlineIndexType, KVIndexType offlineIndexType, FieldType valueType, bool isVarLen,
                   bool hasTTL, bool fileCompress, bool compactHashKey, bool shortOffset, bool useCompactBucket,
                   size_t valueLen, CompressTypeFlag compressTypeFlag,
                   indexlib::config::CompressTypeOption compressType)
    : onlineIndexType(onlineIndexType)
    , offlineIndexType(offlineIndexType)
    , valueType(valueType)
    , isVarLen(isVarLen)
    , hasTTL(hasTTL)
    , fileCompress(fileCompress)
    , compactHashKey(compactHashKey)
    , shortOffset(shortOffset)
    , useCompactBucket(useCompactBucket)
    , valueLen(valueLen)
    , compressTypeFlag(compressTypeFlag)
    , compressType(compressType)
{
}

std::string KVTypeId::ToString() const
{
    std::string str;
    str += std::string("onlineIndexType=") + PrintKVIndexType(onlineIndexType) + ",";
    str += std::string("offlineIndexType=") + PrintKVIndexType(offlineIndexType) + ",";
    str += std::string("valueType=") + std::to_string(valueType) + ",";
    str += std::string("isVarLen=") + PrintBool(isVarLen) + ",";
    str += std::string("hasTTL=") + PrintBool(hasTTL) + ",";
    str += std::string("fileCompress=") + PrintBool(fileCompress) + ",";
    str += std::string("compactHashKey=") + PrintBool(compactHashKey) + ",";
    str += std::string("shortOffset=") + PrintBool(shortOffset) + ",";
    str += std::string("useCompactBucket=") + PrintBool(useCompactBucket) + ",";
    str += std::string("valueLen=") + std::to_string(valueLen) + ",";
    str += std::string("compressTypeFlag=") + std::to_string(compressTypeFlag) + ",";
    str += std::string("compressType=") + compressType.GetCompressStr();
    return str;
}

bool KVTypeId::operator==(const KVTypeId& other) const
{
    return onlineIndexType == other.onlineIndexType && offlineIndexType == other.offlineIndexType &&
           valueType == other.valueType && isVarLen == other.isVarLen && hasTTL == other.hasTTL &&
           fileCompress == other.fileCompress && compactHashKey == other.compactHashKey &&
           shortOffset == other.shortOffset && useCompactBucket == other.useCompactBucket &&
           valueLen == other.valueLen && compressTypeFlag == other.compressTypeFlag &&
           compressType == other.compressType;
}

bool KVTypeId::operator!=(const KVTypeId& other) const { return !(*this == other); }

KVTypeId MakeKVTypeId(const indexlibv2::config::KVIndexConfig& indexConfig, const KVFormatOptions* kvOptions)
{
    KVTypeId typeId;
    typeId.hasTTL = indexConfig.TTLEnabled();
    const auto& valueConfig = indexConfig.GetValueConfig();
    const auto& indexPreference = indexConfig.GetIndexPreference();
    // TODO(xinfei.sxf) refactor the code
    if (!indexPreference.GetValueParam().IsFixLenAutoInline()) {
        if (valueConfig->IsSimpleValue()) {
            typeId.valueType = valueConfig->GetActualFieldType();
            typeId.isVarLen = false;
        } else {
            typeId.isVarLen = true;
        }
    } else {
        // TODO(xinfei.sxf) refactor the code
        auto valueSize = valueConfig->GetFixedLength();
        if (valueSize > 8 || valueSize < 0) {
            typeId.isVarLen = true;
        } else {
            if (kvOptions != nullptr) {
                typeId.useCompactBucket = kvOptions->UseCompactBucket();
            }
            typeId.valueType = valueConfig->GetFixedLengthValueType();
            // TODO(xinfei.sxf) refactor the code
            typeId.isVarLen = false;
        }
    }

    if (!typeId.isVarLen) {
        const auto& attrConfig = valueConfig->GetAttributeConfig(0);
        assert(attrConfig);
        typeId.compressType = attrConfig->GetCompressType();
        if (typeId.compressType.HasFp16EncodeCompress()) {
            typeId.compressTypeFlag = ct_fp16;
            typeId.valueLen = 2;
        } else if (typeId.compressType.HasInt8EncodeCompress()) {
            typeId.compressTypeFlag = ct_int8;
            typeId.valueLen = 1;
        } else {
            typeId.compressTypeFlag = ct_other;
            typeId.valueLen = indexlib::index::SizeOfFieldType(typeId.valueType);
        }
    }

    // index type
    auto indexType = ParseKVIndexType(indexPreference.GetHashDictParam().GetHashType());
    typeId.offlineIndexType = indexType;
    // always use dense hash for online
    typeId.onlineIndexType = KVIndexType::KIT_DENSE_HASH;

    typeId.fileCompress = indexPreference.GetValueParam().EnableFileCompress();

    typeId.compactHashKey = indexConfig.IsCompactHashKey();
    if (kvOptions != nullptr) {
        typeId.shortOffset = kvOptions->IsShortOffset();
    } else {
        typeId.shortOffset = typeId.isVarLen && indexPreference.GetHashDictParam().HasEnableShortenOffset();
    }

    return typeId;
}

} // namespace indexlibv2::index
