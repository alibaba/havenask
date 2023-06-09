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
#ifndef __INDEXLIB_DOCUMENT_JSON_QUERY_PARSER_H
#define __INDEXLIB_DOCUMENT_JSON_QUERY_PARSER_H

#include <memory>

#include "autil/Log.h"
#include "indexlib/common_define.h"
#include "indexlib/document/query/query_parser.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

class JsonQueryParser : public QueryParser
{
public:
    JsonQueryParser();
    ~JsonQueryParser();

public:
    QueryBasePtr Parse(const std::string& str) override;

private:
    QueryBasePtr Parse(const autil::legacy::Any& any) const;
    QueryBasePtr ParseTermQuery(const autil::legacy::json::JsonMap& jsonMap) const;
    QueryBasePtr ParseSubTermQuery(const autil::legacy::json::JsonMap& jsonMap) const;
    QueryBasePtr ParsePatternQuery(const autil::legacy::json::JsonMap& jsonMap) const;
    QueryBasePtr ParseRangeQuery(const autil::legacy::json::JsonMap& jsonMap) const;
    QueryBasePtr ParseAndQuery(const autil::legacy::json::JsonMap& jsonMap) const;
    QueryBasePtr ParseOrQuery(const autil::legacy::json::JsonMap& jsonMap) const;
    QueryBasePtr ParseNotQuery(const autil::legacy::json::JsonMap& jsonMap) const;
    bool ParseSubQuery(const autil::legacy::json::JsonMap& jsonMap, std::vector<QueryBasePtr>& subQuery) const;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(JsonQueryParser);
}} // namespace indexlib::document

#endif //__INDEXLIB_DOCUMENT_JSON_QUERY_PARSER_H
