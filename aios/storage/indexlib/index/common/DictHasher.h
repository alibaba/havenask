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

#include "autil/Log.h"
#include "autil/legacy/json.h"
#include "indexlib/base/FieldType.h"
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/common/Term.h"
#include "indexlib/index/common/Types.h"
#include "indexlib/util/KeyHasherTyped.h"
#include "indexlib/util/KeyValueMap.h"

namespace indexlib::index {

struct DictIndexType {
    using Type = InvertedIndexType;
};

struct DictFieldType {
    using Type = FieldType;
};

template <typename DictType>
class DictHasher
{
    static_assert(std::is_same_v<DictType, DictIndexType> || std::is_same_v<DictType, DictFieldType>);

public:
    DictHasher() {}
    DictHasher(const util::KeyValueMap& hashFuncInfos, typename DictType::Type type);
    DictHasher(const autil::legacy::json::JsonMap& jsonMap, typename DictType::Type type);
    ~DictHasher() {}

    DictHasher(const DictHasher&) = default;
    DictHasher& operator=(const DictHasher&) = default;

public:
    bool GetHashKey(const Term& term, DictKeyInfo& key) const;
    bool CalcHashKey(const std::string& word, dictkey_t& key) const;

private:
    void Init(const util::KeyValueMap& hashFuncInfos);
    bool TryGetHashInTerm(const Term& term, DictKeyInfo& key) const;

private:
    std::optional<util::LayerTextHasher> _layerHasher;
    typename DictType::Type _type;

private:
    AUTIL_LOG_DECLARE();
};

using TokenHasher = DictHasher<DictFieldType>;
using IndexDictHasher = DictHasher<DictIndexType>;
} // namespace indexlib::index
