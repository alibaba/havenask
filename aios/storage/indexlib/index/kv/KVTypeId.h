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

#include <string>

#include "indexlib/base/FieldType.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/CompressTypeOption.h"
#include "indexlib/index/common/Types.h"
#include "indexlib/index/kv/KVCommonDefine.h"

namespace indexlib::config {
class KVIndexConfig;
}

namespace indexlibv2::config {
class KVIndexConfig;
}

namespace indexlibv2::index {
class KVFormatOptions;

#pragma pack(push, 8)
struct KVTypeId {
    KVIndexType onlineIndexType = KVIndexType::KIT_UNKOWN;
    KVIndexType offlineIndexType = KVIndexType::KIT_UNKOWN;
    FieldType valueType = ft_unknown; // valid when single field
    bool isVarLen = false;
    bool hasTTL = false;
    bool fileCompress = false; // var len used only
    bool compactHashKey = false;
    bool shortOffset = false;
    bool useCompactBucket = false;
    size_t valueLen = 0;                               // valid when simple value
    CompressTypeFlag compressTypeFlag = ct_other;      // valid when simple value
    indexlib::config::CompressTypeOption compressType; // valid when simple value

    KVTypeId() = default;
    KVTypeId(KVIndexType onlineIndexType, KVIndexType offlineIndexType, FieldType valueType, bool isVarLen, bool hasTTL,
             bool fileCompress, bool compactHashKey, bool shortOffset, bool useCompactBucket, size_t valueLen,
             CompressTypeFlag compressTypeFlag, indexlib::config::CompressTypeOption compressType);

    std::string ToString() const;

    bool operator==(const KVTypeId& other) const;
    bool operator!=(const KVTypeId& other) const;
};
#pragma pack(pop)

extern KVTypeId MakeKVTypeId(const indexlibv2::config::KVIndexConfig& indexConfig, const KVFormatOptions* kvOptions);

} // namespace indexlibv2::index
