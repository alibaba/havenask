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
#include "sql/data/SqlQueryRequest.h"

#include <iosfwd>
#include <map>
#include <string.h>
#include <utility>
#include <vector>

#include "autil/DataBuffer.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "sql/common/KvPairParser.h"
#include "sql/common/Log.h"
#include "sql/common/common.h"
#include "sql/data/SqlQueryPattern.h"

using namespace std;
using namespace autil;
namespace sql {

static const string SOURCE_SPEC = "exec.source.spec";

bool SqlQueryRequest::init(const string &queryStr) {
    _rawQueryStr = queryStr;
    vector<string> clauses = StringUtil::split(queryStr, SQL_CLAUSE_SPLIT, true);
    if (clauses.empty()) {
        return false;
    }
    for (auto &clause : clauses) {
        StringUtil::trim(clause);
    }
    for (const auto &clause : clauses) {
        if (clause.find(SQL_KVPAIR_CLAUSE) == 0) {
            _kvPairStr = clause.substr(strlen(SQL_KVPAIR_CLAUSE));
            KvPairParser::parse(_kvPairStr, SQL_KV_CLAUSE_SPLIT, SQL_KV_SPLIT, _kvPair);
        } else if (clause.find(SQL_AUTH_TOKEN_CLAUSE) == 0) {
            _sqlAuthTokenStr = clause.substr(strlen(SQL_AUTH_TOKEN_CLAUSE));
        } else if (clause.find(SQL_AUTH_SIGNATURE_CLAUSE) == 0) {
            _sqlAuthSignatureStr = clause.substr(strlen(SQL_AUTH_SIGNATURE_CLAUSE));
        } else {
            if (clause.find(SQL_QUERY_CLAUSE) == 0) {
                if (!setSqlQueryStr(clause.substr(strlen(SQL_QUERY_CLAUSE)))) {
                    return false;
                }
            } else {
                if (!setSqlQueryStr(clause)) {
                    return false;
                }
            }
        }
    }
    if (!validate()) {
        return false;
    }
    return true;
}

bool SqlQueryRequest::init(const std::string &sqlStr,
                           const std::unordered_map<std::string, std::string> &kvPair) {
    if (!setSqlQueryStr(sqlStr)) {
        return false;
    }
    setSqlParams(kvPair);
    if (!validate()) {
        return false;
    }
    return true;
}

bool SqlQueryRequest::setSqlQueryStr(const std::string &sqlStr) {
    if (!_sqlStr.empty()) {
        SQL_LOG(ERROR,
                "request all ready has query [%s], to set [%s]",
                _sqlStr.c_str(),
                sqlStr.c_str());
        return false;
    }
    _sqlStr = sqlStr;
    StringUtil::trim(_sqlStr);
    _keyword = _sqlStr.substr(0, 6);
    StringUtil::toLowerCase(_keyword);
    if (_keyword == "select") {
        _sqlType = SQL_TYPE_DQL;
    } else if (_keyword == "delete" || _keyword == "update" || _keyword == "insert") {
        _sqlType = SQL_TYPE_DML;
    } else {
        _sqlType = SQL_TYPE_UNKNOWN;
        _keyword = "unknown";
    }
    return true;
}

void SqlQueryRequest::setSqlParams(const std::unordered_map<std::string, std::string> &kvPair) {
    _kvPair = kvPair;
}

const std::string &SqlQueryRequest::getSourceSpec() {
    static const string defaultSource = SQL_SOURCE_SPEC_EMPTY;
    return getValueWithDefault(SOURCE_SPEC, defaultSource);
}

const std::string &SqlQueryRequest::getDatabaseName() {
    static const string defaultDb = "unknown";
    return getValueWithDefault(SQL_DATABASE_NAME, defaultDb);
}

bool SqlQueryRequest::getValue(const string &name, string &value) const {
    auto iter = _kvPair.find(name);
    if (iter == _kvPair.end()) {
        return false;
    }
    value = iter->second;
    return true;
}

const string &SqlQueryRequest::getValueWithDefault(const string &name, const string &value) const {
    auto iter = _kvPair.find(name);
    if (iter == _kvPair.end()) {
        return value;
    }
    return iter->second;
}

bool SqlQueryRequest::getBoolKey(const std::string &key) const {
    std::string info;
    getValue(key, info);
    if (info.empty()) {
        return false;
    }
    bool res = false;
    autil::StringUtil::fromString(info, res);
    return res;
}

bool SqlQueryRequest::isOutputSqlPlan() const {
    return getBoolKey(SQL_GET_SQL_PLAN);
}

bool SqlQueryRequest::isResultReadable() const {
    return getBoolKey(SQL_RESULT_READABLE);
}

autil::CompressType SqlQueryRequest::getResultCompressType() const {
    std::string compressType;
    getValue(SQL_GET_RESULT_COMPRESS_TYPE, compressType);
    if (!compressType.empty()) {
        auto convertedType = autil::convertCompressType(compressType);
        if (convertedType == autil::CompressType::INVALID_COMPRESS_TYPE) {
            SQL_LOG(
                WARN, "Parse result compress type failed: invalid type [%s]", compressType.c_str());
            return autil::CompressType::NO_COMPRESS;
        }
        return convertedType;
    }
    return autil::CompressType::NO_COMPRESS;
}

bool SqlQueryRequest::validate() {
    if (_sqlStr.empty()) {
        SQL_LOG(WARN, "sql query is empty!");
        return false;
    } else {
        return true;
    }
}

const std::string &SqlQueryRequest::toString() {
    if (_rawQueryStr.empty()) {
        string kvPairStr = "&&kvpair=";
        for (const auto &kv : _kvPair) {
            kvPairStr += kv.first + ":" + kv.second + ";";
        }
        _rawQueryStr = "query=" + _sqlStr + kvPairStr;
    }
    return _rawQueryStr;
}

void SqlQueryRequest::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_version);
    dataBuffer.write(_sqlStr);
    dataBuffer.write(_kvPair);
}

void SqlQueryRequest::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_version);
    dataBuffer.read(_sqlStr);
    dataBuffer.read(_kvPair);
}

std::string SqlQueryRequest::toPatternString(size_t queryHashKey) const {
    SqlQueryPattern pattern;
    pattern.queryHashKey = queryHashKey;
    pattern.sqlStr = _sqlStr;
    pattern._kvPair.insert(_kvPair.begin(), _kvPair.end());
    return pattern.toCompactJson();
}

} // namespace sql
