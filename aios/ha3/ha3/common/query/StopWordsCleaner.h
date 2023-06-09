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

#include <stddef.h>
#include <memory>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/Query.h"
#include "ha3/common/QueryVisitor.h"

namespace isearch {
namespace common {
class AndNotQuery;
class AndQuery;
class MultiTermQuery;
class NumberQuery;
class OrQuery;
class PhraseQuery;
class RankQuery;
class TermQuery;
}  // namespace common
}  // namespace isearch

namespace isearch {
namespace qrs {

class StopWordsCleaner : public common::QueryVisitor
{
public:
    StopWordsCleaner();
    ~StopWordsCleaner();
private:
    StopWordsCleaner(const StopWordsCleaner &other);
    StopWordsCleaner &operator = (const StopWordsCleaner &);
public:
    bool clean(const common::Query *query) {
        reset();
        query->accept(this); 
        return _errorCode == ERROR_NONE;
    }
    ErrorCode getErrorCode() const {
        return _errorCode;
    }
    const std::string& getErrorMsg() const {
        return _errorMsg;
    }

    void visitTermQuery(const common::TermQuery *query);
    void visitPhraseQuery(const common::PhraseQuery *query);
    void visitAndQuery(const common::AndQuery *query);
    void visitOrQuery(const common::OrQuery *query);
    void visitAndNotQuery(const common::AndNotQuery *query);
    void visitRankQuery(const common::RankQuery *query);
    void visitNumberQuery(const common::NumberQuery *query);
    void visitMultiTermQuery(const common::MultiTermQuery *query);
   
    common::Query* stealQuery() {
        common::Query *tmpQuery = _visitedQuery;
        _visitedQuery = NULL;
        return tmpQuery;
    }
private:
    void visitAndOrQuery(common::Query *resultQuery, const common::Query *srcQuery);
    void visitAndNotRankQuery(common::Query *resultQuery, const common::Query *srcQuery);
    inline void cleanVisitQuery() {
        if (_visitedQuery) {
            delete _visitedQuery;
            _visitedQuery = NULL;
        }
    }
    void reset() {
        _errorCode = ERROR_NONE;
        _errorMsg = "";
    }
    void setError(ErrorCode ec, const std::string &errorMsg) {
        _errorCode = ec;
        _errorMsg = errorMsg;
    }
private:
    common::Query *_visitedQuery;
    ErrorCode _errorCode;
    std::string _errorMsg;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<StopWordsCleaner> StopWordsCleanerPtr;

} // namespace qrs
} // namespace isearch

