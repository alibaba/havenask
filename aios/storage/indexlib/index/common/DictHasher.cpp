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
#include "indexlib/index/common/DictHasher.h"

#include "autil/StringUtil.h"
#include "indexlib/index/common/KeyHasherWrapper.h"
#include "indexlib/util/KeyValueMap.h"

using namespace std;

namespace indexlib::index {
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, DictHasher, DictType);

template <typename DictType>
DictHasher<DictType>::DictHasher(const util::KeyValueMap& hashFuncInfos, typename DictType::Type type)
{
    _type = type;
    if constexpr (is_same_v<DictType, DictIndexType>) {
        if (type == it_text || type == it_string) {
            Init(hashFuncInfos);
        }
    } else if constexpr (is_same_v<DictType, DictFieldType>) {
        if (type == ft_text || type == ft_string) {
            Init(hashFuncInfos);
        }
    }
}

template <typename DictType>
DictHasher<DictType>::DictHasher(const autil::legacy::json::JsonMap& jsonMap, typename DictType::Type type)
{
    _type = type;
    util::KeyValueMap hashFuncInfos = util::ConvertFromJsonMap(jsonMap);

    if constexpr (is_same_v<DictType, DictIndexType>) {
        if (type == it_text || type == it_string) {
            Init(hashFuncInfos);
        }
    } else if constexpr (is_same_v<DictType, DictFieldType>) {
        if (type == ft_text || type == ft_string) {
            Init(hashFuncInfos);
        }
    }
}

template <typename DictType>
void DictHasher<DictType>::Init(const util::KeyValueMap& hashFuncInfos)
{
    string funcName = util::GetValueFromKeyValueMap(hashFuncInfos, "dict_hash_func", "default");
    autil::StringUtil::toLowerCase(funcName);
    if (funcName == "layerhash") {
        _layerHasher = make_optional<util::LayerTextHasher>(hashFuncInfos);
    }
}

template <typename DictType>
bool DictHasher<DictType>::TryGetHashInTerm(const Term& term, DictKeyInfo& key) const
{
    if (term.IsNull()) {
        key = DictKeyInfo::NULL_TERM;
        return true;
    }

    dictkey_t hashKey;
    if (term.GetTermHashKey(hashKey)) {
        key = DictKeyInfo(hashKey);
        return true;
    }
    return false;
}

template <typename DictType>
bool DictHasher<DictType>::GetHashKey(const Term& term, DictKeyInfo& key) const
{
    if (TryGetHashInTerm(term, key)) {
        return true;
    }

    dictkey_t hashKey;
    if (CalcHashKey(term.GetWord(), hashKey)) {
        const_cast<index::Term&>(term).SetTermHashKey(hashKey);
        key = DictKeyInfo(hashKey);
        return true;
    }
    return false;
}

template <typename DictType>
bool DictHasher<DictType>::CalcHashKey(const std::string& word, dictkey_t& key) const
{
    if constexpr (is_same_v<DictType, DictIndexType>) {
        return _layerHasher ? _layerHasher->GetHashKey(word, key)
                            : KeyHasherWrapper::GetHashKeyByInvertedIndexType(_type, word.c_str(), word.size(), key);
    } else {
        return _layerHasher ? _layerHasher->GetHashKey(word, key)
                            : KeyHasherWrapper::GetHashKeyByFieldType(_type, word.c_str(), word.size(), key);
    }
}

template class DictHasher<DictFieldType>;
template class DictHasher<DictIndexType>;

} // namespace indexlib::index
