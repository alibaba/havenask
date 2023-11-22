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
#include "build_service/util/SourceFieldExtractorUtil.h"

#include <vector>

#include "alog/Logger.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "build_service/util/Log.h"
#include "indexlib/base/Constant.h"

namespace build_service::util {

BS_LOG_SETUP(build_service.util, SourceFieldExtractorUtil);

bool SourceFieldExtractorUtil::needExtractSourceField(
    const std::shared_ptr<indexlibv2::config::FieldConfig>& fieldConfig)
{
    auto userDefinedParam = fieldConfig->GetUserDefinedParam();
    if (userDefinedParam.empty()) {
        return false;
    }
    auto sourceField = userDefinedParam.find(SOURCE_FIELD);
    if (sourceField == userDefinedParam.end()) {
        return false;
    }
    return true;
}

bool SourceFieldExtractorUtil::getValue(const SourceFieldParsedInfo& parsedInfo, const SourceFieldParam& param,
                                        std::string& val)
{
    if (!param.needConcatAllKeys()) {
        const auto& kvMap = parsedInfo.second;
        auto it = kvMap.find(param.key);
        if (it == kvMap.end()) {
            return false;
        }
        val = it->second;
    } else {
        // concate all keys with tokenizeSeparator
        for (const auto& [k, v] : parsedInfo.second) {
            if (!val.empty()) {
                val += param.tokenizeSeparator;
            }
            val += k;
        }
    }
    return true;
}

bool SourceFieldExtractorUtil::checkUnifiedSeparator(const SourceFieldParam& param,
                                                     SourceField2Separators& sourceField2Separators)
{
    auto iter = sourceField2Separators.find(param.fieldName);
    if (iter != sourceField2Separators.end()) {
        const auto& separators = iter->second;
        if (param.kvPairSeparator != separators.first || param.kvSeparator != separators.second) {
            BS_LOG(ERROR, "check unified separator failed.");
            return false;
        }
        return true;
    }
    sourceField2Separators.insert(
        iter, std::make_pair(param.fieldName, std::make_pair(param.kvPairSeparator, param.kvSeparator)));
    return true;
}

// attributes = ;asdpTimingExpress:true;ppay:1;fip:11.81.235.58;7d:1;j13:1;newDO:1;
// for example, KEY equal to the field of newD0, so the fieldValue of 1 is expected
bool SourceFieldExtractorUtil::parseRawString(const SourceFieldParam& param, SourceFieldParsedInfo& parsedInfo)
{
    const std::string& rawString = parsedInfo.first;
    UnorderedStringViewKVMap& kvMap = parsedInfo.second;
    auto kvPairs = autil::StringTokenizer::tokenizeView(rawString, param.kvPairSeparator,
                                                        autil::StringTokenizer::TOKEN_LEAVE_AS |
                                                            autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
    std::string allKeys;
    for (const auto& kvPair : kvPairs) {
        auto values =
            autil::StringTokenizer::tokenizeView(kvPair, param.kvSeparator, autil::StringTokenizer::TOKEN_LEAVE_AS);
        // format like: key:val
        if (values.size() != 2) {
            BS_INTERVAL_LOG2(60, ERROR, "invalid kv pair format: %s", rawString.c_str());
            return false;
        }

        autil::StringUtil::trim(values[0]);
        kvMap[values[0]] = values[1];
    }
    return true;
}

bool SourceFieldExtractorUtil::getSourceFieldParam(const std::shared_ptr<indexlibv2::config::FieldConfig>& fieldConfig,
                                                   SourceFieldParam& param)
{
    auto userDefinedMap = fieldConfig->GetUserDefinedParam();
    auto sourceField = userDefinedMap[util::SourceFieldExtractorUtil::SOURCE_FIELD]; // must exist in map
    try {
        auto jsonMap = autil::legacy::AnyCast<autil::legacy::json::JsonMap>(sourceField);
        for (const auto& [k, v] : jsonMap) {
            auto val = autil::legacy::AnyCast<std::string>(v);
            if (k == FIELD_NAME) {
                param.fieldName = val;
            } else if (k == KEY) {
                param.key = val;
            } else if (k == KV_PAIR_SEPARATOR) {
                param.kvPairSeparator = val;
            } else if (k == KV_SEPARATOR) {
                param.kvSeparator = val;
            } else if (k == TOKENIZE_SEPARATOR) {
                param.tokenizeSeparator = val;
            }
        }
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "AnyCast failed, errorMsg [%s]", e.what());
        return false;
    }
    if (!param.tokenizeSeparator.empty()) {
        if (!param.key.empty()) {
            BS_LOG(ERROR, "invalid source field param, tokenize-separator and key cannot exist at the same time.");
            return false;
        }
    } else {
        if (param.key.empty()) {
            param.tokenizeSeparator = indexlib::MULTI_VALUE_SEPARATOR;
        }
    }
    return true;
}
bool SourceFieldExtractorUtil::getSourceFieldParam(const std::shared_ptr<indexlibv2::config::FieldConfig>& fieldConfig,
                                                   std::shared_ptr<SourceFieldParam>& param)
{
    return getSourceFieldParam(fieldConfig, *param);
}
} // namespace build_service::util
