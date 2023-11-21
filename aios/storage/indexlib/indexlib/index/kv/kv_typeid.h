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

#include "indexlib/index/kv/KVCommonDefine.h"
#include "indexlib/index/kv/kv_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

#pragma pack(push, 8)
struct KVTypeId {
public:
    KVTypeId()
        : onlineIndexType(indexlibv2::index::KVIndexType::KIT_UNKOWN)
        , offlineIndexType(indexlibv2::index::KVIndexType::KIT_UNKOWN)
        , valueType(ft_unknown)
        , isVarLen(false)
        , hasTTL(false)
        , fileCompress(false)
        , compactHashKey(false)
        , shortOffset(false)
        , multiRegion(false)
        , useCompactBucket(false)
    {
    }

    KVTypeId(indexlibv2::index::KVIndexType _onlineIndexType, indexlibv2::index::KVIndexType _offlineIndexType,
             int8_t _valueType, bool _isVarLen, bool _hasTTL, bool _fileCompress = false,
             bool _useCompactBucket = false)
        : onlineIndexType(_onlineIndexType)
        , offlineIndexType(_offlineIndexType)
        , valueType(_valueType)
        , isVarLen(_isVarLen)
        , hasTTL(_hasTTL)
        , fileCompress(_fileCompress)
        , compactHashKey(false)
        , shortOffset(false)
        , multiRegion(false)
        , useCompactBucket(_useCompactBucket)
    {
    }

public:
    indexlibv2::index::KVIndexType onlineIndexType;
    indexlibv2::index::KVIndexType offlineIndexType;
    int8_t valueType; // valid when single field
    bool isVarLen;
    bool hasTTL;
    bool fileCompress; // var len used only
    bool compactHashKey;
    bool shortOffset;
    bool multiRegion;
    bool useCompactBucket;
};
#pragma pack(pop)
}} // namespace indexlib::index
