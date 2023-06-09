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
#include "indexlib/document/query/json_query_parser.h"

#include "indexlib/document/query/and_query.h"
#include "indexlib/document/query/not_query.h"
#include "indexlib/document/query/or_query.h"
#include "indexlib/document/query/pattern_query.h"
#include "indexlib/document/query/query_function.h"
#include "indexlib/document/query/range_query.h"
#include "indexlib/document/query/sub_term_query.h"
#include "indexlib/document/query/term_query.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, JsonQueryParser);

JsonQueryParser::JsonQueryParser() {}

JsonQueryParser::~JsonQueryParser() {}

QueryBasePtr JsonQueryParser::Parse(const string& str)
{
    Any any = ParseJson(str);
    return Parse(any);
}

QueryBasePtr JsonQueryParser::Parse(const autil::legacy::Any& any) const
{
    JsonMap queryMap = AnyCast<JsonMap>(any);
    JsonMap::iterator iter = queryMap.find("type");
    if (iter == queryMap.end()) {
        AUTIL_LOG(ERROR, "type key is missing [%s].", ToString(any).c_str());
        return QueryBasePtr();
    }

    string typeStr = AnyCast<string>(iter->second);
    QueryType type = QueryBase::StringToQueryType(typeStr);
    QueryBasePtr ret;
    switch (type) {
    case QT_TERM: {
        ret = ParseTermQuery(queryMap);
        break;
    }
    case QT_SUB_TERM: {
        ret = ParseSubTermQuery(queryMap);
        break;
    }
    case QT_RANGE: {
        ret = ParseRangeQuery(queryMap);
        break;
    }
    case QT_PATTERN: {
        ret = ParsePatternQuery(queryMap);
        break;
    }
    case QT_AND: {
        ret = ParseAndQuery(queryMap);
        break;
    }
    case QT_OR: {
        ret = ParseOrQuery(queryMap);
        break;
    }
    case QT_NOT: {
        ret = ParseNotQuery(queryMap);
        break;
    }
    default:
        assert(false);
    }
    return ret;
}

QueryBasePtr JsonQueryParser::ParseTermQuery(const JsonMap& jsonMap) const
{
    auto iter = jsonMap.find("value");
    if (iter == jsonMap.end()) {
        AUTIL_LOG(ERROR, "value not exist for TermQuery [%s].", ToJsonString(jsonMap).c_str());
        return QueryBasePtr();
    }
    string fieldValue = AnyCast<string>(iter->second);

    string fieldName;
    iter = jsonMap.find("field");
    if (iter != jsonMap.end()) {
        fieldName = AnyCast<string>(iter->second);
    }

    auto funcIter = jsonMap.find("function");
    if (iter == jsonMap.end() && funcIter == jsonMap.end()) {
        AUTIL_LOG(ERROR, "field or function not exist for TermQuery [%s].", ToJsonString(jsonMap).c_str());
        return QueryBasePtr();
    }

    TermQueryPtr ret(new TermQuery);
    if (funcIter == jsonMap.end()) {
        ret->Init(fieldName, fieldValue);
    } else {
        string funcStr = AnyCast<string>(funcIter->second);
        QueryFunction function;
        if (!function.FromString(funcStr)) {
            AUTIL_LOG(ERROR, "invalid function string [%s]", funcStr.c_str());
            return QueryBasePtr();
        }
        if (!fieldName.empty()) {
            function.SetAliasField(fieldName);
        }
        ret->Init(function, fieldValue);
    }
    return ret;
}

QueryBasePtr JsonQueryParser::ParseSubTermQuery(const JsonMap& jsonMap) const
{
    auto iter = jsonMap.find("value");
    if (iter == jsonMap.end()) {
        AUTIL_LOG(ERROR, "value not exist for SubTermQuery [%s].", ToJsonString(jsonMap).c_str());
        return QueryBasePtr();
    }
    string subValue = AnyCast<string>(iter->second);

    string fieldName;
    iter = jsonMap.find("field");
    if (iter != jsonMap.end()) {
        fieldName = AnyCast<string>(iter->second);
    }

    auto funcIter = jsonMap.find("function");
    if (iter == jsonMap.end() && funcIter == jsonMap.end()) {
        AUTIL_LOG(ERROR, "field or function not exist for SubTermQuery [%s].", ToJsonString(jsonMap).c_str());
        return QueryBasePtr();
    }

    SubTermQueryPtr ret(new SubTermQuery);
    if (funcIter == jsonMap.end()) {
        iter = jsonMap.find("delimeter");
        if (iter == jsonMap.end()) {
            ret->Init(fieldName, subValue);
        } else {
            string delimeter = AnyCast<string>(iter->second);
            ret->Init(fieldName, subValue, delimeter);
        }
    } else {
        string funcStr = AnyCast<string>(funcIter->second);
        QueryFunction function;
        if (!function.FromString(funcStr)) {
            AUTIL_LOG(ERROR, "invalid function string [%s]", funcStr.c_str());
            return QueryBasePtr();
        }
        if (!fieldName.empty()) {
            function.SetAliasField(fieldName);
        }
        iter = jsonMap.find("delimeter");
        if (iter == jsonMap.end()) {
            ret->Init(function, subValue);
        } else {
            string delimeter = AnyCast<string>(iter->second);
            ret->Init(function, subValue, delimeter);
        }
    }
    return ret;
}

QueryBasePtr JsonQueryParser::ParsePatternQuery(const JsonMap& jsonMap) const
{
    auto iter = jsonMap.find("pattern");
    if (iter == jsonMap.end()) {
        AUTIL_LOG(ERROR, "pattern not exist for PatternQuery [%s].", ToJsonString(jsonMap).c_str());
        return QueryBasePtr();
    }
    string fieldPattern = AnyCast<string>(iter->second);

    string fieldName;
    iter = jsonMap.find("field");
    if (iter != jsonMap.end()) {
        fieldName = AnyCast<string>(iter->second);
    }

    auto funcIter = jsonMap.find("function");
    if (iter == jsonMap.end() && funcIter == jsonMap.end()) {
        AUTIL_LOG(ERROR, "field or function not exist for PatternQuery [%s].", ToJsonString(jsonMap).c_str());
        return QueryBasePtr();
    }

    PatternQueryPtr ret(new PatternQuery);
    if (funcIter == jsonMap.end()) {
        ret->Init(fieldName, fieldPattern);
    } else {
        string funcStr = AnyCast<string>(funcIter->second);
        QueryFunction function;
        if (!function.FromString(funcStr)) {
            AUTIL_LOG(ERROR, "invalid function string [%s]", funcStr.c_str());
            return QueryBasePtr();
        }
        if (!fieldName.empty()) {
            function.SetAliasField(fieldName);
        }
        ret->Init(function, fieldPattern);
    }
    return ret;
}

QueryBasePtr JsonQueryParser::ParseRangeQuery(const JsonMap& jsonMap) const
{
    auto iter = jsonMap.find("value");
    if (iter == jsonMap.end()) {
        AUTIL_LOG(ERROR, "value not exist for RangeQuery [%s].", ToJsonString(jsonMap).c_str());
        return QueryBasePtr();
    }
    string rangeValue = AnyCast<string>(iter->second);

    string fieldName;
    iter = jsonMap.find("field");
    if (iter != jsonMap.end()) {
        fieldName = AnyCast<string>(iter->second);
    }

    auto funcIter = jsonMap.find("function");
    if (iter == jsonMap.end() && funcIter == jsonMap.end()) {
        AUTIL_LOG(ERROR, "field or function not exist for RangeQuery [%s].", ToJsonString(jsonMap).c_str());
        return QueryBasePtr();
    }

    RangeQueryPtr ret(new RangeQuery);
    if (funcIter == jsonMap.end()) {
        ret->Init(fieldName, rangeValue);
    } else {
        string funcStr = AnyCast<string>(funcIter->second);
        QueryFunction function;
        if (!function.FromString(funcStr)) {
            AUTIL_LOG(ERROR, "invalid function string [%s]", funcStr.c_str());
            return QueryBasePtr();
        }
        if (!fieldName.empty()) {
            function.SetAliasField(fieldName);
        }
        ret->Init(function, rangeValue);
    }
    return ret;
}

bool JsonQueryParser::ParseSubQuery(const JsonMap& jsonMap, vector<QueryBasePtr>& subQuery) const
{
    auto it = jsonMap.find("sub_query");
    JsonArray regionVector = AnyCast<JsonArray>(it->second);
    for (JsonArray::iterator iter = regionVector.begin(); iter != regionVector.end(); ++iter) {
        QueryBasePtr query = Parse(*iter);
        if (!query) {
            AUTIL_LOG(ERROR, "Parse query fail [%s]", ToString(*iter).c_str());
            return false;
        }
        subQuery.push_back(query);
    }
    return true;
}

QueryBasePtr JsonQueryParser::ParseAndQuery(const JsonMap& jsonMap) const
{
    vector<QueryBasePtr> subQuery;
    if (!ParseSubQuery(jsonMap, subQuery)) {
        return QueryBasePtr();
    }
    AndQueryPtr ret(new AndQuery);
    ret->Init(subQuery);
    return ret;
}

QueryBasePtr JsonQueryParser::ParseOrQuery(const JsonMap& jsonMap) const
{
    vector<QueryBasePtr> subQuery;
    if (!ParseSubQuery(jsonMap, subQuery)) {
        return QueryBasePtr();
    }
    OrQueryPtr ret(new OrQuery);
    ret->Init(subQuery);
    return ret;
}

QueryBasePtr JsonQueryParser::ParseNotQuery(const JsonMap& jsonMap) const
{
    vector<QueryBasePtr> subQuery;
    if (!ParseSubQuery(jsonMap, subQuery)) {
        return QueryBasePtr();
    }
    if (subQuery.size() != 1) {
        AUTIL_LOG(ERROR, "subQuery size is not 1, subQuery[%s].", ToJsonString(jsonMap).c_str());
        return QueryBasePtr();
    }
    NotQueryPtr ret(new NotQuery);
    ret->Init(subQuery);
    return ret;
}
}} // namespace indexlib::document
