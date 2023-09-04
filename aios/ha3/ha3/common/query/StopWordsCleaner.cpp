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
#include "ha3/common/StopWordsCleaner.h"

#include <assert.h>
#include <cstddef>
#include <memory>
#include <vector>

#include "autil/Log.h"
#include "build_service/analyzer/Token.h"
#include "ha3/common/AndNotQuery.h"
#include "ha3/common/AndQuery.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/MultiTermQuery.h"
#include "ha3/common/NumberQuery.h"
#include "ha3/common/OrQuery.h"
#include "ha3/common/PhraseQuery.h"
#include "ha3/common/RankQuery.h"
#include "ha3/common/Term.h"
#include "ha3/common/TermQuery.h"

namespace isearch {
namespace qrs {
AUTIL_LOG_SETUP(ha3, StopWordsCleaner);

using namespace isearch::common;
using namespace std;
StopWordsCleaner::StopWordsCleaner() {
    _visitedQuery = NULL;
    reset();
}

StopWordsCleaner::~StopWordsCleaner() {
    cleanVisitQuery();
}

void StopWordsCleaner::visitTermQuery(const TermQuery *query) {
    cleanVisitQuery();
    if (query && !(query->getTerm().getToken().isStopWord())) {
        _visitedQuery = new TermQuery(*query);
    }
}

void StopWordsCleaner::visitPhraseQuery(const PhraseQuery *query) {
    cleanVisitQuery();
    if (query) {
        const Query::TermArray &termArray = query->getTermArray();
        Query::TermArray termVec;
        for (size_t i = 0; i < termArray.size(); ++i) {
            if (!termArray[i]->getToken().isStopWord()) {
                termVec.push_back(termArray[i]);
            }
        }
        if (termVec.size() > 1) {
            _visitedQuery = new PhraseQuery(*query);
        } else if (termVec.size() == 1) {
            _visitedQuery = new TermQuery(*termVec[0], query->getQueryLabel());
        }
    }
}

void StopWordsCleaner::visitMultiTermQuery(const MultiTermQuery *query) {
    cleanVisitQuery();
    if (query) {
        const Query::TermArray &termArray = query->getTermArray();
        vector<size_t> termIndexs;
        for (size_t i = 0; i < termArray.size(); ++i) {
            if (!termArray[i]->getToken().isStopWord()) {
                termIndexs.push_back(i);
            }
        }
        size_t termSize = termIndexs.size();
        if (termSize == 1) {
            _visitedQuery = new TermQuery(*termArray[termIndexs[0]], query->getQueryLabel());
            return;
        } else if (termSize > 1) {
            if (termSize == termArray.size()) {
                _visitedQuery = new MultiTermQuery(*query);
                return;
            }

            MultiTermQuery *multi = new MultiTermQuery(query->getQueryLabel(), query->getOpExpr());
            for (size_t i = 0; i < termIndexs.size(); ++i) {
                multi->addTerm(termArray[termIndexs[i]]);
            }
            _visitedQuery = multi;
        }
    }
}

void StopWordsCleaner::visitAndQuery(const AndQuery *query) {
    cleanVisitQuery();
    if (query) {
        AndQuery *resultQuery = new AndQuery(query->getQueryLabel());
        visitAndOrQuery(resultQuery, query);
    }
}

void StopWordsCleaner::visitOrQuery(const OrQuery *query) {
    cleanVisitQuery();
    if (query) {
        OrQuery *resultQuery = new OrQuery(query->getQueryLabel());
        visitAndOrQuery(resultQuery, query);
    }
}

void StopWordsCleaner::visitAndNotQuery(const AndNotQuery *query) {
    cleanVisitQuery();
    if (query) {
        AndNotQuery *resultQuery = new AndNotQuery(query->getQueryLabel());
        visitAndNotRankQuery(resultQuery, query);
    }
}

void StopWordsCleaner::visitRankQuery(const RankQuery *query) {
    cleanVisitQuery();
    if (query) {
        RankQuery *resultQuery = new RankQuery(query->getQueryLabel());
        visitAndNotRankQuery(resultQuery, query);
    }
}

void StopWordsCleaner::visitNumberQuery(const NumberQuery *query) {
    cleanVisitQuery();
    if (query) {
        _visitedQuery = new NumberQuery(*query);
    }
}

void StopWordsCleaner::visitAndOrQuery(common::Query *resultQuery, const common::Query *srcQuery) {
    const vector<QueryPtr> *childQueryPtr = srcQuery->getChildQuery();
    assert(childQueryPtr);
    vector<Query *> restQuery;
    size_t childNum = childQueryPtr->size();
    Query *query = NULL;
    for (size_t i = 0; i < childNum; ++i) {
        if ((*childQueryPtr)[i]) {
            query = NULL;
            (*childQueryPtr)[i]->accept(this);
            query = stealQuery();
            if (query) {
                restQuery.push_back(query);
            }
        }
    }
    if (_errorCode != ERROR_NONE) {
        for (vector<Query *>::iterator it = restQuery.begin(); it != restQuery.end(); ++it) {
            delete (*it);
        }
        delete resultQuery;
        _visitedQuery = NULL;
        return;
    }
    if (restQuery.size() > 1) {
        for (size_t j = 0; j < restQuery.size(); ++j) {
            QueryPtr queryPtr(restQuery[j]);
            resultQuery->addQuery(queryPtr);
        }
        _visitedQuery = resultQuery;
    } else if (1 == restQuery.size()) {
        delete resultQuery;
        _visitedQuery = restQuery[0];
        if (_visitedQuery->getQueryLabel().empty()) {
            _visitedQuery->setQueryLabelWithDefaultLevel(srcQuery->getQueryLabel());
        }
    } else {
        delete resultQuery;
        _visitedQuery = NULL;
    }
}

void StopWordsCleaner::visitAndNotRankQuery(common::Query *resultQuery,
                                            const common::Query *srcQuery) {
    const vector<QueryPtr> *childQueryPtr = srcQuery->getChildQuery();
    assert(childQueryPtr);
    vector<Query *> rightQuery;
    size_t childNum = childQueryPtr->size();
    Query *query = NULL;
    Query *leftQuery = NULL;
    if (childNum > 0) {
        if ((*childQueryPtr)[0]) {
            (*childQueryPtr)[0]->accept(this);
            leftQuery = stealQuery();
        }
    }

    if ((_errorCode != ERROR_NONE) || (NULL == leftQuery)) {
        delete resultQuery;
        _visitedQuery = NULL;
        if (ERROR_NONE == _errorCode) {
            setError(ERROR_QUERY_INVALID,
                     "AndNotQuery or RankQuery's "
                     "leftQuery is NULL or stop word.");
        }
        return;
    }

    for (size_t i = 1; i < childNum; ++i) {
        if ((*childQueryPtr)[i]) {
            query = NULL;
            (*childQueryPtr)[i]->accept(this);
            query = stealQuery();
            if (query) {
                rightQuery.push_back(query);
            }
        }
    }
    if (_errorCode != ERROR_NONE) {
        for (vector<Query *>::iterator it = rightQuery.begin(); it != rightQuery.end(); ++it) {
            delete (*it);
        }
        delete leftQuery;
        delete resultQuery;
        _visitedQuery = NULL;
        return;
    }

    if (rightQuery.size() > 0) {
        QueryPtr leftPtr(leftQuery);
        resultQuery->addQuery(leftPtr);
        for (size_t j = 0; j < rightQuery.size(); ++j) {
            QueryPtr rightPtr(rightQuery[j]);
            resultQuery->addQuery(rightPtr);
        }
        _visitedQuery = resultQuery;
    } else {
        delete resultQuery;
        _visitedQuery = leftQuery;
        if (leftQuery->getQueryLabel().empty()) {
            _visitedQuery->setQueryLabelWithDefaultLevel(srcQuery->getQueryLabel());
        }
    }
}

} // namespace qrs
} // namespace isearch
