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

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/legacy/jsonizable.h"
#include "ha3/isearch.h"

namespace isearch {
namespace common {

class QueryInfo : public autil::legacy::Jsonizable {
public:
    QueryInfo();
    QueryInfo(const std::string &defaultIndexName,
              QueryOperator defaultOP = OP_AND,
              bool flag = false);
    ~QueryInfo();

public:
    const std::string &getDefaultIndexName() const {
        return _defaultIndexName;
    }
    void setDefaultIndexName(const std::string &indexName) {
        _defaultIndexName = indexName;
    }

    QueryOperator getDefaultOperator() const {
        return _defaultOP;
    }
    void setDefaultOperator(QueryOperator defaultOP) {
        _defaultOP = defaultOP;
    }
    bool getDefaultMultiTermOptimizeFlag() const {
        return _useMultiTermOptimize;
    }
    void setMultiTermOptimizeFlag(bool flag) {
        _useMultiTermOptimize = flag;
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);

private:
    std::string _defaultIndexName;
    QueryOperator _defaultOP;
    bool _useMultiTermOptimize;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<QueryInfo> QueryInfoPtr;

} // namespace common
} // namespace isearch
