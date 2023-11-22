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
#include "indexlib/config/MutableJson.h"

#include <algorithm>
#include <assert.h>
#include <map>
#include <vector>

#include "MutableJson.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/StringView.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"

namespace indexlibv2::config {

AUTIL_LOG_SETUP(indexlib.config, MutableJson);

const autil::legacy::Any* MutableJson::GetAny(const std::string& jsonPath) const noexcept
{
    auto [ret, nodeHints] = ParseJsonPath(jsonPath);
    if (!ret) {
        return nullptr;
    }
    if (_json.IsEmpty()) {
        return nullptr;
    }
    return Locate</*readOnly*/ true>(nodeHints.begin(), nodeHints.end(), &_json);
}

bool MutableJson::SetValue(const std::string& jsonPath, const char* value)
{
    std::string valueStr(value);
    return SetValue(jsonPath, valueStr);
}

bool MutableJson::SetAny(const std::string& jsonPath, const autil::legacy::Any& anyValue)
{
    auto [ret, nodeHints] = ParseJsonPath(jsonPath);
    if (!ret) {
        return false;
    }
    if (_json.IsEmpty()) {
        _json = autil::legacy::json::JsonMap();
    }
    auto any = Locate</*readOnly*/ false>(nodeHints.begin(), nodeHints.end(), &_json);
    if (!any) {
        AUTIL_LOG(ERROR, "locate path[%s] failed", jsonPath.c_str());
        return false;
    }
    *any = anyValue;
    return true;
}

std::pair<bool, std::vector<MutableJson::JsonNodeHint>> MutableJson::ParseJsonPath(const std::string& jsonPath)
{
    std::vector<std::string> jsonPathVec = autil::StringTokenizer::tokenize(autil::StringView(jsonPath), '.',
                                                                            autil::StringTokenizer::TOKEN_IGNORE_EMPTY |
                                                                                autil::StringTokenizer::TOKEN_TRIM);
    std::vector<JsonNodeHint> nodeHints;
    for (const auto& rawSection : jsonPathVec) {
        size_t arrayStart = rawSection.find('[');
        bool isArray = (arrayStart != std::string::npos);
        std::string nodeName = isArray ? rawSection.substr(0, arrayStart) : rawSection;
        JsonNodeHint hint;
        hint.nodeName = nodeName;
        hint.isArray = isArray;
        nodeHints.push_back(std::move(hint));

        while (arrayStart != std::string::npos) {
            size_t arrayEnd = rawSection.find(']', arrayStart);
            if (arrayEnd == std::string::npos) {
                AUTIL_LOG(ERROR, "syntax error on jsonPath[%s], expect array end", jsonPath.c_str());
                return {false, {}};
            }
            std::string idxStr = rawSection.substr(arrayStart + 1, arrayEnd - arrayStart - 1);
            size_t idx = 0;
            if (!autil::StringUtil::fromString<size_t>(idxStr, idx)) {
                AUTIL_LOG(ERROR, "syntax error on jsonPath[%s], expect array idx, but [%s]", jsonPath.c_str(),
                          idxStr.c_str());
                return {false, {}};
            }
            JsonNodeHint hint;
            hint.idxInArray = idx;
            arrayStart = rawSection.find('[', arrayEnd + 1);
            hint.isArray = (arrayStart != std::string::npos);
            nodeHints.push_back(std::move(hint));
        }
    }
    return {true, std::move(nodeHints)};
}

bool MutableJson::ContainerTypeMatched(const autil::legacy::Any* any, bool isArray)
{
    if (!any) {
        return false;
    }
    return isArray ? (autil::legacy::AnyCast<autil::legacy::json::JsonArray>(any) != nullptr)
                   : (autil::legacy::AnyCast<autil::legacy::json::JsonMap>(any) != nullptr);
}

template <bool readOnly, typename NodeIter, typename AnyType>
AnyType* MutableJson::Locate(NodeIter nodeIter, NodeIter nodeIterEnd, AnyType* parent)
{
    if (nodeIter == nodeIterEnd) {
        return parent;
    }
    JsonNodeHint hint = *nodeIter;
    bool isLastNode = (nodeIter + 1 == nodeIterEnd);
    AnyType* any = nullptr;
    if (hint.idxInArray) {
        auto jsonArray = autil::legacy::AnyCast<autil::legacy::json::JsonArray>(parent);
        assert(jsonArray);
        auto idx = hint.idxInArray.value();
        if (idx < jsonArray->size()) {
            any = std::addressof((*jsonArray)[idx]);
        } else if constexpr (!readOnly) {
            jsonArray->resize(idx + 1);
            any = std::addressof((*jsonArray)[idx]);
        }
    } else {
        auto jsonMap = autil::legacy::AnyCast<autil::legacy::json::JsonMap>(parent);
        auto iter = jsonMap->find(hint.nodeName);
        if (iter != jsonMap->end()) {
            any = std::addressof(iter->second);
        } else if constexpr (!readOnly) {
            iter = jsonMap->insert(iter, {hint.nodeName, autil::legacy::Any()});
            any = std::addressof(iter->second);
        }
    }

    if (!any || (!isLastNode && !ContainerTypeMatched(any, hint.isArray))) {
        if constexpr (readOnly) {
            AUTIL_LOG(DEBUG, "key[%s] not exist", hint.nodeName.c_str());
            return nullptr;
        } else {
            assert(any);
            if (hint.isArray) {
                *any = autil::legacy::json::JsonArray();
            } else {
                *any = autil::legacy::json::JsonMap();
            }
        }
    }
    return Locate<readOnly>(nodeIter + 1, nodeIterEnd, any);
}

} // namespace indexlibv2::config
