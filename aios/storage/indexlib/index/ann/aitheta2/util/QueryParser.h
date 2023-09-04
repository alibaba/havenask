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
#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "indexlib/index/ann/aitheta2/AithetaQueryWrapper.h"

namespace indexlibv2::index::ann {
class QueryParser
{
public:
    static Status Parse(const AithetaIndexConfig& indexConf, const std::string& termWord,
                        AithetaQueries& indexQuery);

private:
    static Status ParseFromRawString(const AithetaIndexConfig& indexConf, const std::string& termWord,
                                     AithetaQueryWrapper& queryWrapper);
    template <typename T>
    static Status ParseValue(std::string& queryStr, const std::string& key, T& value);
    static Status ParseEmbedding(const AithetaIndexConfig& indexConf, const std::string& queryStr,
                                 std::map<index_id_t, std::vector<float>>& embeddingMap);

private:
    inline static const std::string QUERY_SEARCH_PARAMS_KEY = "&search_params=";
    AUTIL_LOG_DECLARE();
};

template <typename T>
Status QueryParser::ParseValue(std::string& queryStr, const std::string& key, T& value)
{
    auto tokens = autil::StringUtil::split(queryStr, key);
    if (tokens.size() == 2) {
        if (!autil::StringUtil::fromString(tokens[1], value)) {
            RETURN_STATUS_ERROR(InvalidArgs, "parse[%s] failed", tokens[1].c_str());
        }
    } else if (tokens.size() != 1) {
        RETURN_STATUS_ERROR(InvalidArgs, "parse key[%s] with[%s] failed as format error", key.c_str(),
                            queryStr.c_str());
    }
    queryStr = tokens[0];
    return Status::OK();
}

typedef std::shared_ptr<QueryParser> QueryParserPtr;
} // namespace indexlibv2::index::ann
