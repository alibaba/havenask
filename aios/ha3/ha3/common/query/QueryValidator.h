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
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/Query.h"
#include "ha3/common/QueryVisitor.h"
#include "ha3/common/Term.h"

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
namespace suez {
namespace turing {
class IndexInfos;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace qrs {

class QueryValidator : public common::QueryVisitor
{
public:
    QueryValidator(const suez::turing::IndexInfos *indexInfos);
    virtual ~QueryValidator();
public:
    bool validate(const common::Query *query) {
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
    virtual void visitTermQuery(const common::TermQuery *query);
    virtual void visitPhraseQuery(const common::PhraseQuery *query);
    virtual void visitAndQuery(const common::AndQuery *query);
    virtual void visitOrQuery(const common::OrQuery *query);
    virtual void visitAndNotQuery(const common::AndNotQuery *query);
    virtual void visitRankQuery(const common::RankQuery *query);
    virtual void visitNumberQuery(const common::NumberQuery *query);
    virtual void visitMultiTermQuery(const common::MultiTermQuery *query);
private:
    bool validateIndexName(const std::string &indexName);
    bool validateIndexFields(const std::string &indexName,
                             const common::RequiredFields &requiredFields);
    void validateAdvancedQuery(const common::Query* query);
    void validateTerms(const std::vector<common::TermPtr> &termArray,
                       const ErrorCode ec, std::string &errorMsg,
                       bool validateTermEmpty = true);

template<typename T>
    bool checkNullQuery(const T* query);
private:
    void reset();
private:
    const suez::turing::IndexInfos *_indexInfos;
    ErrorCode _errorCode;
    std::string _errorMsg;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<QueryValidator> QueryValidatorPtr;

} // namespace qrs
} // namespace isearch

