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

#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/config/FieldConfig.h"

namespace build_service::util {

using SourceField2Separators =
    std::map<std::string, std::pair<std::string, std::string>>; // fieldName -> {kv_pair_sep, kv_sep}
using UnorderedStringViewKVMap = std::unordered_map<std::string_view, std::string_view>;
using SourceFieldParsedInfo = std::pair<std::string, UnorderedStringViewKVMap>;
using SourceFieldMap = std::map<std::string, SourceFieldParsedInfo>;

class SourceFieldExtractorUtil : private autil::NoCopyable
{
public:
    static constexpr const char* SOURCE_FIELD = "source_field";
    static constexpr const char* FIELD_NAME = "field_name";
    static constexpr const char* KEY = "key";
    static constexpr const char* KV_PAIR_SEPARATOR = "kv_pair_separator";
    static constexpr const char* KV_SEPARATOR = "kv_separator";
    static constexpr const char* TOKENIZE_SEPARATOR = "tokenize_separator";

    struct SourceFieldParam {
        std::string kvPairSeparator;
        std::string kvSeparator;
        std::string tokenizeSeparator;
        std::string fieldName;
        std::string key;
        bool operator==(const SourceFieldParam& o) const
        {
            return kvPairSeparator == o.kvPairSeparator && kvSeparator == o.kvSeparator &&
                   tokenizeSeparator == o.tokenizeSeparator && fieldName == o.fieldName && key == o.key;
        }
        bool needConcatAllKeys() const { return !tokenizeSeparator.empty(); }
    };

public:
    SourceFieldExtractorUtil() = default;
    ~SourceFieldExtractorUtil() = default;

    static bool needExtractSourceField(const std::shared_ptr<indexlibv2::config::FieldConfig>& fieldConfig);
    static bool checkUnifiedSeparator(const SourceFieldParam& param, SourceField2Separators& sourceField2Separators);
    static bool getValue(const SourceFieldParsedInfo& parsedInfo, const SourceFieldParam& param, std::string& val);
    static bool parseRawString(const SourceFieldParam& param, SourceFieldParsedInfo& parsedInfo);
    static bool getSourceFieldParam(const std::shared_ptr<indexlibv2::config::FieldConfig>& fieldConfig,
                                    std::shared_ptr<SourceFieldParam>& param);
    static bool getSourceFieldParam(const std::shared_ptr<indexlibv2::config::FieldConfig>& fieldConfig,
                                    SourceFieldParam& param);
    AUTIL_LOG_DECLARE();
};

} // namespace build_service::util
