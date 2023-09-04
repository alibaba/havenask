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
#include "ha3/common/TermCombineVisitor.h"

#include <assert.h>
#include <cstddef>
#include <memory>
#include <typeinfo>
#include <vector>

#include "autil/Log.h"
#include "ha3/common/AndNotQuery.h"
#include "ha3/common/AndQuery.h"
#include "ha3/common/MultiTermQuery.h"
#include "ha3/common/NumberQuery.h"
#include "ha3/common/OrQuery.h"
#include "ha3/common/PhraseQuery.h"
#include "ha3/common/Query.h"
#include "ha3/common/RankQuery.h"
#include "ha3/common/Term.h"
#include "ha3/common/TermQuery.h"
#include "ha3/isearch.h"
#include "indexlib/config/ITabletSchema.h"

using namespace std;
using namespace isearch::common;

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, TermCombineVisitor);

const std::string TermCombineVisitor::DEFAULT_TOKEN_CONNECTOR = "_";

TermCombineVisitor::TermCombineVisitor(const autil::legacy::json::JsonMap &param) {
    _visitedQuery = NULL;
    createDict(param);
}

TermCombineVisitor::~TermCombineVisitor() {
    DELETE_AND_SET_NULL(_visitedQuery);
}

/*
"field_combine" : [
  {
    "combine_index_name" : "phrase_category",
    "combine_index_param" ; "phrase;category",
    "payload_field" : "phrase",
    "token_connector" : "-"
  },
  {
    "combine_index_name" : "phrase_brand",
    "combine_index_param" ; "phrase;brand",
    "payload_field" : "phrase",
    "token_connector" : "-"
  }
]
*/

void TermCombineVisitor::createDict(const autil::legacy::json::JsonMap &param) {
    try {
        auto iter = param.find("field_combine");
        if (iter != param.end()) {
            if (autil::legacy::json::IsJsonArray(iter->second)) {
                auto combineKeyMapArr
                    = autil::legacy::AnyCast<autil::legacy::json::JsonArray>(iter->second);
                for (const auto &key : combineKeyMapArr) {
                    if (autil::legacy::json::IsJsonMap(key)) {
                        auto keyMap = autil::legacy::AnyCast<autil::legacy::json::JsonMap>(key);
                        auto indexNameIter = keyMap.find("combine_index_name");
                        auto fieldNamesIter = keyMap.find("combine_index_param");
                        auto tokenConnectorIter = keyMap.find("token_connector");
                        if (indexNameIter == keyMap.end() || fieldNamesIter == keyMap.end()) {
                            return;
                        } else {
                            string indexName
                                = autil::legacy::AnyCast<string>(indexNameIter->second);
                            string fieldName
                                = autil::legacy::AnyCast<string>(fieldNamesIter->second);
                            string tokenSplit = DEFAULT_TOKEN_CONNECTOR;
                            if (tokenConnectorIter != keyMap.end()) {
                                tokenSplit
                                    = autil::legacy::AnyCast<string>(tokenConnectorIter->second);
                            }
                            _combineDict.push_back(make_tuple(
                                indexName, autil::StringUtil::split(fieldName, ";"), tokenSplit));
                        }
                    }
                }
            }
        }
    } catch (const exception &e) { return; }
}

void TermCombineVisitor::visitTermQuery(const TermQuery *query) {
    assert(!_visitedQuery);
    _visitedQuery = new TermQuery(*query);
    _singleTerms = {OP_AND, query->getTerm()};
}

void TermCombineVisitor::visitPhraseQuery(const PhraseQuery *query) {
    assert(!_visitedQuery);
    _visitedQuery = new PhraseQuery(*query);
    _singleTerms.reset();
}

void TermCombineVisitor::visitMultiTermQuery(const MultiTermQuery *query) {
    assert(!_visitedQuery);
    _visitedQuery = new MultiTermQuery(*query);
    _singleTerms.reset();
    if (query->getOpExpr() != OP_AND && query->getOpExpr() != OP_OR) {
        return;
    }
    _singleTerms.op = query->getOpExpr();
    const Query::TermArray &termArray = query->getTermArray();
    for (const auto &term : termArray) {
        _singleTerms.terms.push_back(*term.get());
    }
}

void TermCombineVisitor::visitAndQuery(const AndQuery *query) {
    vector<QueryPtr> childQueryVec;
    vector<SingleTerms> childQuerySingleTermsVec;
    const vector<QueryPtr> *childrenQuery = query->getChildQuery();
    for (const auto &childQuery : *childrenQuery) {
        childQuery->accept(this);
        QueryPtr query(stealQuery());
        childQueryVec.emplace_back(query);
        childQuerySingleTermsVec.push_back(_singleTerms);
    }

    AndQuery *resultQuery(new AndQuery(query->getQueryLabel()));
    bool flag = true;
    vector<bool> masks(childQueryVec.size(), 0);
    while (flag) {
        flag = false;
        for (const auto &combineInfo : _combineDict) {
            vector<SingleTerms> matchedResult;
            if (match(childQuerySingleTermsVec, std::get<1>(combineInfo), masks, matchedResult)) {
                createCombineIndex(
                    matchedResult, std::get<0>(combineInfo), std::get<2>(combineInfo), resultQuery);
                flag = true;
                break;
            }
        }
    }

    for (size_t i = 0; i < childQueryVec.size(); ++i) {
        if (!masks[i]) {
            resultQuery->addQuery(childQueryVec[i]);
        }
    }
    _visitedQuery = resultQuery;
    _singleTerms.reset();
}

bool TermCombineVisitor::match(const vector<SingleTerms> &childQuerySingleTerms,
                               const vector<std::string> &fields,
                               vector<bool> &masks,
                               vector<SingleTerms> &matchedResult) {
    vector<size_t> idxs;
    for (size_t i = 0; i < fields.size(); ++i) {
        bool firstMatched = true;
        for (size_t idx = 0; idx < childQuerySingleTerms.size(); ++idx) {
            if (!masks[idx] && childQuerySingleTerms[idx].terms.size() > 0
                && childQuerySingleTerms[idx].terms[0].getIndexName() == fields[i]) {
                if (firstMatched) {
                    firstMatched = false;
                    matchedResult.push_back(childQuerySingleTerms[idx]);
                    idxs.push_back(idx);
                } else {
                    auto &lastResult = matchedResult.back();
                    if (lastResult.op == childQuerySingleTerms[idx].op && lastResult.op == OP_AND) {
                        // todo intersection
                        lastResult.terms.insert(lastResult.terms.end(),
                                                childQuerySingleTerms[idx].terms.begin(),
                                                childQuerySingleTerms[idx].terms.end());
                        idxs.push_back(idx);
                    }
                }
            }
        }
        if (firstMatched) {
            return false;
        }
    }
    size_t expansionSize = 1;
    size_t expansionCnt = 0;
    for (const auto &res : matchedResult) {
        expansionSize *= res.terms.size();
        expansionCnt += res.terms.size() > 1;
    }
    if (expansionSize > 100 || expansionCnt > 2) {
        return false;
    }
    for (auto idx : idxs) {
        masks[idx] = true;
    }
    return true;
}

void TermCombineVisitor::getWordsFromMatchedResult(const vector<SingleTerms> &matchedResult,
                                                   vector<string> &words,
                                                   const string &tokenConnector) {
    vector<int> idxs(matchedResult.size(), 0);
    size_t expansionCnt = 1;
    for (const auto &res : matchedResult) {
        expansionCnt *= res.terms.size();
    }
    vector<string> tokenConnectorTmpVec = {"", tokenConnector};
    for (int i = 0; i < expansionCnt; ++i) {
        string concatWord("");
        for (int j = 0; j < idxs.size(); ++j) {
            concatWord += tokenConnectorTmpVec[j != 0] + matchedResult[j].terms[idxs[j]].getWord();
        }
        words.emplace_back(concatWord);
        for (int j = matchedResult.size() - 1; j >= 0; --j) {
            if (++idxs[j] == matchedResult[j].terms.size()) {
                idxs[j] = 0;
            } else {
                break;
            }
        }
    }
}

// 0_0: a AND x AND 1 -> ax1
// 1_1: a&b&c AND x -> ax&bx&cx
// 1_0: a|b|c AND x -> ax|bx|cx
// 2_3: a&b&c AND 1&2&3 -> a1&a2&a3&b1&b2&b3&c1&c2&c3
// 2_1: a&b&c AND 1|2|3 -> (a1|a2|a3)&(b1|b2|b3)&(c1|c2|c3)
// 2_0: a|b|c AND 1|2|3 -> a1|a2|a3|b1|b2|b3|c1|c2|c3
// 2_2: a|b|c AND x AND 1&2&3 -> (ax1|bx1|cx1)&(ax2|bx2|cx2)&(ax3|bx3|cx3)
void TermCombineVisitor::createCombineIndex(const vector<SingleTerms> &matchedResult,
                                            const std::string &indexName,
                                            const std::string &tokenConnector,
                                            AndQuery *resultQuery) {

    size_t pattern = 0;
    size_t patternCnt = 0;
    size_t patternSize[2];
    for (const auto &res : matchedResult) {
        if (res.terms.size() > 1) {
            pattern += (1 << patternCnt) * (OP_AND == res.op);
            patternSize[patternCnt++] = res.terms.size();
        }
    }

    vector<string> words;
    getWordsFromMatchedResult(matchedResult, words, tokenConnector);

    if (patternCnt == 0 || (patternCnt == 1 && pattern == 1) || (patternCnt == 2 && pattern == 3)) {
        MultiTermQueryPtr andQuery(new MultiTermQuery("", OP_AND));
        for (size_t i = 0; i < words.size(); ++i) {
            RequiredFields requiredFields;
            TermPtr term(new Term(words[i], indexName, requiredFields));
            andQuery->addTerm(term);
        }
        resultQuery->addQuery(andQuery);
    } else if (patternCnt == 2 && pattern == 1) {
        for (size_t i = 0; i < patternSize[0]; ++i) {
            MultiTermQueryPtr orQuery(new MultiTermQuery("", OP_OR));
            for (size_t j = 0; j < patternSize[1]; ++j) {
                size_t idx = i * patternSize[1] + j;
                RequiredFields requiredFields;
                TermPtr term(new Term(words[idx], indexName, requiredFields));
                orQuery->addTerm(term);
            }
            resultQuery->addQuery(orQuery);
        }
    } else if (patternCnt == 2 && pattern == 2) {
        for (size_t j = 0; j < patternSize[1]; ++j) {
            MultiTermQueryPtr orQuery(new MultiTermQuery("", OP_OR));
            for (size_t i = 0; i < patternSize[0]; ++i) {
                size_t idx = i * patternSize[1] + j;
                RequiredFields requiredFields;
                TermPtr term(new Term(words[idx], indexName, requiredFields));
                orQuery->addTerm(term);
            }
            resultQuery->addQuery(orQuery);
        }
    } else if ((patternCnt == 1 && pattern == 0) || (patternCnt == 2 && pattern == 0)) {
        MultiTermQueryPtr orQuery(new MultiTermQuery("", OP_OR));
        for (size_t i = 0; i < words.size(); ++i) {
            RequiredFields requiredFields;
            TermPtr term(new Term(words[i], indexName, requiredFields));
            orQuery->addTerm(term);
        }
        resultQuery->addQuery(orQuery);
    }
}

template <class T>
void TermCombineVisitor::visitChildren(const T *query) {
    const std::vector<QueryPtr> *childrenQuery = query->getChildQuery();
    T *resultQuery(new T(query->getQueryLabel()));
    for (const auto &childQuery : *childrenQuery) {
        childQuery->accept(this);
        QueryPtr query(stealQuery());
        resultQuery->addQuery(query);
    }
    _visitedQuery = resultQuery;
    _singleTerms.reset();
}

void TermCombineVisitor::visitOrQuery(const OrQuery *query) {
    visitChildren(query);
}

void TermCombineVisitor::visitAndNotQuery(const AndNotQuery *query) {
    visitChildren(query);
}

void TermCombineVisitor::visitRankQuery(const RankQuery *query) {
    visitChildren(query);
}

void TermCombineVisitor::visitNumberQuery(const NumberQuery *query) {
    _visitedQuery = new NumberQuery(*query);
    _singleTerms = {OP_AND, query->getTerm()};
}

Query *TermCombineVisitor::stealQuery() {
    Query *tmpQuery = _visitedQuery;
    _visitedQuery = NULL;
    return tmpQuery;
}

template void TermCombineVisitor::visitChildren<OrQuery>(const OrQuery *);
template void TermCombineVisitor::visitChildren<AndNotQuery>(const AndNotQuery *);
template void TermCombineVisitor::visitChildren<RankQuery>(const RankQuery *);
} // namespace common
} // namespace isearch
