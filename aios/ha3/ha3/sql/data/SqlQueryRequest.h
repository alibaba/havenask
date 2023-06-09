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

#include <memory>
#include <string>
#include <unordered_map>

#include "autil/DataBuffer.h"
#include "autil/Log.h"

namespace isearch {
namespace sql {
class SqlAuthManager;

enum SQL_TYPE {
    SQL_TYPE_UNKNOWN = 0,
    SQL_TYPE_DQL = 1,
    SQL_TYPE_DML = 2,
};

class SqlQueryRequest {
public:
    SqlQueryRequest() {}

public:
    bool init(const std::string &queryStr);
    bool init(const std::string &sqlStr,
              const std::unordered_map<std::string, std::string> &kvPair);
    bool getValue(const std::string &name, std::string &value) const;
    const std::string &getValueWithDefault(const std::string &name,
                                           const std::string &value = "") const;
    const std::string &getSqlQuery() const {
        return _sqlStr;
    }
    const std::unordered_map<std::string, std::string> &getSqlParams() const {
        return _kvPair;
    }
    SQL_TYPE getSqlType() const {
        return _sqlType;
    }
    const std::string &getKeyword() const {
        return _keyword;
    }
    const std::string &getSourceSpec();
    const std::string &getDatabaseName();
    bool validate();
    bool operator==(const SqlQueryRequest &rhs) const {
        return _version == rhs._version && _sqlStr == rhs._sqlStr && _kvPair == rhs._kvPair;
    }
    bool checkAuth(const SqlAuthManager &manager) const;
    const std::string &toString();

public:
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);
    std::string toPatternString(size_t queryHashKey) const;

public:
    static void parseKvPair(const std::string &value,
                            std::unordered_map<std::string, std::string> &kvPair);
private:
    bool setSqlQueryStr(const std::string &sqlStr);
    void setSqlParams(const std::unordered_map<std::string, std::string> &kvPair);

private:
    int16_t _version = 0;
    std::string _sqlStr;
    std::string _kvPairStr;
    std::string _keyword;
    std::string _sqlAuthTokenStr;
    std::string _sqlAuthSignatureStr;
    std::unordered_map<std::string, std::string> _kvPair;
    SQL_TYPE _sqlType = SQL_TYPE_UNKNOWN;
    std::string _rawQueryStr;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SqlQueryRequest> SqlQueryRequestPtr;
} // namespace sql
} // namespace isearch
