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
#include "ha3/common/QueryValidator.h"

#include <assert.h>
#include <memory>
#include <sstream>
#include <stddef.h>
#include <stdint.h>

#include "autil/Log.h"
#include "ha3/common/AndNotQuery.h"
#include "ha3/common/AndQuery.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/MultiTermQuery.h"
#include "ha3/common/NumberQuery.h"
#include "ha3/common/NumberTerm.h"
#include "ha3/common/OrQuery.h"
#include "ha3/common/PhraseQuery.h"
#include "ha3/common/Query.h"
#include "ha3/common/RankQuery.h"
#include "ha3/common/TermQuery.h"
#include "indexlib/indexlib.h"
#include "suez/turing/expression/util/IndexInfos.h"

using namespace std;
using namespace suez::turing;
using namespace isearch::common;

namespace isearch {
namespace qrs {
AUTIL_LOG_SETUP(ha3, QueryValidator);

QueryValidator::QueryValidator(const IndexInfos *indexInfos) {
    _indexInfos = indexInfos;
    reset();
}

QueryValidator::~QueryValidator() {}

bool QueryValidator::validateIndexName(const string &indexName) {
    assert(_indexInfos);
    const IndexInfo *indexInfo = _indexInfos->getIndexInfo(indexName.c_str());
    if (NULL == indexInfo) {
        _errorCode = ERROR_INDEX_NOT_EXIST;
        ostringstream oss;
        oss << "indexName:[" << indexName << "] not exist";
        _errorMsg = oss.str();
        return false;
    }

    return true;
}

bool QueryValidator::validateIndexFields(const string &indexName,
                                         const RequiredFields &requiredFields) {
    if (requiredFields.fields.empty()) {
        return true;
    }

    const IndexInfo *indexInfo = _indexInfos->getIndexInfo(indexName.c_str());
    assert(indexInfo);
    if (it_expack != indexInfo->getIndexType()) {
        _errorCode = ERROR_INDEX_TYPE_INVALID;
        ostringstream oss;
        oss << "Index type invalid, indexName:[" << indexName << "]"
            << ", indexType:[" << indexInfo->getIndexType() << "]";
        _errorMsg = oss.str();
        return false;
    }

    vector<string>::const_iterator it = requiredFields.fields.begin();
    for (; it != requiredFields.fields.end(); ++it) {
        if (-1 == indexInfo->getFieldPosition((*it).c_str())) {
            _errorCode = ERROR_INDEX_FIELD_INVALID;
            ostringstream oss;
            oss << "Index type invalid, indexName:[" << indexName << "]"
                << ", invalidField:[" << *it << "]";
            _errorMsg = oss.str();
            return false;
        }
    }

    return true;
}

void QueryValidator::validateAdvancedQuery(const Query *query) {
    const vector<QueryPtr> &child = *(query->getChildQuery());
    for (size_t i = 0; i < child.size(); ++i) {
        if (checkNullQuery(child[i].get()))
            return;
        child[i]->accept(this);
        if (_errorCode != ERROR_NONE) {
            return;
        }
    }
}

void QueryValidator::visitTermQuery(const TermQuery *query) {
    if (checkNullQuery(query)) {
        return;
    }
    const string &indexName = query->getTerm().getIndexName();
    if (!validateIndexName(indexName)) {
        return;
    }
    validateIndexFields(indexName, query->getTerm().getRequiredFields());
}

void QueryValidator::visitMultiTermQuery(const MultiTermQuery *query) {
    if (checkNullQuery(query)) {
        return;
    }
    const Query::TermArray &termArray = query->getTermArray();
    string errorMsg = "QueryClause: invalid term(&|) query";
    validateTerms(termArray, ERROR_NULL_MULTI_TERM_QUERY, errorMsg, false);
}

void QueryValidator::visitPhraseQuery(const PhraseQuery *query) {
    if (checkNullQuery(query)) {
        return;
    }
    const Query::TermArray &termArray = query->getTermArray();
    string errorMsg = "QueryClause: null phrase query";
    validateTerms(termArray, ERROR_NULL_PHRASE_QUERY, errorMsg);
}

void QueryValidator::validateTerms(const vector<TermPtr> &termArray,
                                   const ErrorCode ec,
                                   string &errorMsg,
                                   bool validateTermEmpty /*=true*/) {
    if (termArray.empty()) {
        _errorCode = ec;
        _errorMsg = errorMsg;
        return;
    }
    if (validateTermEmpty) {
        for (vector<TermPtr>::const_iterator it = termArray.begin(); it != termArray.end(); ++it) {
            if ((*it)->getWord().empty()) {
                _errorCode = ec;
                _errorMsg = errorMsg;
                return;
            }
        }
    }

    const string &indexName = termArray[0]->getIndexName();
    if (!validateIndexName(indexName)) {
        return;
    }
    validateIndexFields(indexName, termArray[0]->getRequiredFields());
}

void QueryValidator::visitAndQuery(const AndQuery *query) {
    if (checkNullQuery(query))
        return;
    validateAdvancedQuery(query);
}

void QueryValidator::visitOrQuery(const OrQuery *query) {
    if (checkNullQuery(query))
        return;
    validateAdvancedQuery(query);
}

void QueryValidator::visitAndNotQuery(const AndNotQuery *query) {
    if (checkNullQuery(query))
        return;
    validateAdvancedQuery(query);
}

void QueryValidator::visitRankQuery(const RankQuery *query) {
    if (checkNullQuery(query))
        return;
    validateAdvancedQuery(query);
}

void QueryValidator::visitNumberQuery(const NumberQuery *query) {
    if (checkNullQuery(query))
        return;
    const NumberTerm &term = query->getTerm();
    int64_t leftNum = term.getLeftNumber();
    int64_t rightNum = term.getRightNumber();
    if (rightNum < leftNum) {
        _errorCode = ERROR_NUMBER_RANGE_ERROR;
        ostringstream oss;
        oss << "Number range invalid, leftNumber:[" << leftNum << "]"
            << ", rightNumber:[" << rightNum << "]";
        _errorMsg = oss.str();
        return;
    }

    const string &indexName = term.getIndexName();
    if (!validateIndexName(indexName)) {
        return;
    }
    validateIndexFields(indexName, query->getTerm().getRequiredFields());
}

template <typename T>
bool QueryValidator::checkNullQuery(const T *query) {
    if (query == NULL) {
        _errorCode = ERROR_NULL_QUERY;
        _errorMsg = "Query is null";
        return true;
    }
    return false;
}

void QueryValidator::reset() {
    _errorCode = ERROR_NONE;
    _errorMsg = "";
}

} // namespace qrs
} // namespace isearch
