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
#include "ha3/qrs/CheckMatchVisitor.h"

#include <assert.h>
#include <stdint.h>
#include <algorithm>
#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "ha3/common/AndNotQuery.h"
#include "ha3/common/AndQuery.h"
#include "ha3/common/MultiTermQuery.h"
#include "ha3/common/NumberQuery.h"
#include "ha3/common/NumberTerm.h"
#include "ha3/common/OrQuery.h"
#include "ha3/common/PhraseQuery.h"
#include "ha3/common/Query.h"
#include "ha3/common/RankQuery.h"
#include "ha3/common/Term.h"
#include "ha3/common/TermQuery.h"
#include "ha3/isearch.h"

using namespace std;
using namespace isearch::common;

namespace isearch {
namespace qrs {
AUTIL_LOG_SETUP(ha3, CheckMatchVisitor);

CheckMatchVisitor::CheckMatchVisitor(const string &traceInfo)
    : _isInANDNOTPart(false)
{
    parseMatchInfo(traceInfo);
}

CheckMatchVisitor::~CheckMatchVisitor() {
}

void CheckMatchVisitor::parseMatchInfo(const string &traceInfoStr) {
    if (traceInfoStr.empty()) {
        return;
    }
    static const string matchSepStr = "###||###";
    static const string sepStr = "|||";
    static constexpr char sep = ':';
    static constexpr char matchTrue = '1';
    static const uint32_t sepSize = matchSepStr.size();
    size_t posBeg = traceInfoStr.find(matchSepStr);
    size_t posEnd = traceInfoStr.rfind(matchSepStr);
    if (posBeg == string::npos || posEnd == string::npos) {
        return;
    }
    string allMatches = traceInfoStr.substr(posBeg + sepSize, posEnd - posBeg - sepSize);
    const vector<string> &matchInfoVec =
        autil::StringUtil::split(allMatches, sepStr, true);
    for (size_t i = 0; i < matchInfoVec.size(); i++) {
        const string &str = matchInfoVec[i];
        size_t pos = str.rfind(sep);
        if (pos != string::npos) {
            const string &termStr = str.substr(0, pos);
            if (str.size() > pos + 1 && str[pos + 1] == matchTrue) {
                _matchInfoMap[termStr] = true;
            } else {
                _matchInfoMap[termStr] = false;
            }
        }
    }
}

void CheckMatchVisitor::visitTermQuery(const TermQuery *query){
    const Term& term = query->getTerm();
    checkTermMatch(term);
}

void CheckMatchVisitor::visitMultiTermQuery(const MultiTermQuery *query){
    const MultiTermQuery::TermArray &termArray = query->getTermArray();
    QueryOperator op = query->getOpExpr();
    uint32_t matchCount = 0;
    CheckResultInfo info;
    for (size_t i = 0; i < termArray.size(); ++i) {
        bool match = checkTermMatch(*termArray[i]);
        if (match) {
            ++matchCount;
            if (op == OP_OR) {
                _checkResult.reset();
                return;
            }
        } else {
            if (op == OP_AND) {
                return;
            } else if (op == OP_WEAKAND && !info.errorState) {
                info = _checkResult;
            }
        }
    }
    if (op == OP_WEAKAND) {
        if (matchCount >= query->getMinShouldMatch()) {
            _checkResult.reset();
        } else {
            _checkResult = info;
        }
    }
}

void CheckMatchVisitor::visitPhraseQuery(const PhraseQuery *query) {
    const PhraseQuery::TermArray &termArray = query->getTermArray();
    string phraseStr, indexName;
    for (size_t i = 0; i < termArray.size(); ++i) {
        phraseStr += termArray[i]->getWord().c_str() ;
        phraseStr.append(" ");
        indexName = termArray[i]->getIndexName();
        if(!checkTermMatch(*termArray[i])){
            break;
        }
    }
    if(!_checkResult.errorState){
        autil::StringUtil::trim(phraseStr);
       _phraseQueryMap[phraseStr] = indexName;
    }
}

void CheckMatchVisitor::visitAndQuery(const AndQuery *query){
    const vector<QueryPtr>* children = query->getChildQuery();
    assert(children);
    for (size_t i = 0; i < children->size(); ++i) {
        (*children)[i]->accept(this);
        if(_checkResult.errorState){
            break;
        }
    }
}

void CheckMatchVisitor::visitOrQuery(const OrQuery *query){
    checkAtLeastOneMatch(query);
}

void CheckMatchVisitor::visitAndNotQuery(const AndNotQuery *query){
    const vector<QueryPtr>* children = query->getChildQuery();
    assert(children);
    assert(children->size() > 0);
    (*children)[0]->accept(this);
    if(_checkResult.errorState){
        return;
    }
    _isInANDNOTPart = true;
    for(size_t i = 1; i < children->size(); i++){
        (*children)[i]->accept(this);
        if(_checkResult.errorState){
            break;
        }
    }
    _isInANDNOTPart = false;
}

void CheckMatchVisitor::visitRankQuery(const RankQuery *query) {
    checkAtLeastOneMatch(query);
}

void CheckMatchVisitor::visitNumberQuery(const NumberQuery *query){
    const NumberTerm& term = query->getTerm();
    checkTermMatch(term);
}

bool CheckMatchVisitor::checkTermMatch(const Term& term){
    const string &keyStr = term.getIndexName() + ":" + term.getWord().c_str();
    map<string, bool>::const_iterator iter = _matchInfoMap.find(keyStr);
    bool matched = (iter != _matchInfoMap.end() && iter->second);
    if ((!matched && !_isInANDNOTPart) || (matched && _isInANDNOTPart)) {
        _checkResult.errorState = true;
        _checkResult.errorType = _isInANDNOTPart ? MCE_NOT_QUERY : MCE_TOKENIZE;
        _checkResult.wordStr = term.getWord().c_str();
        _checkResult.indexStr = term.getIndexName();
        return false;
    }
    return true;
}

bool CheckMatchVisitor::checkAtLeastOneMatch(const Query *query) {
    const vector<QueryPtr>* children = query->getChildQuery();
    assert(children);
    if (children->empty()) {
        return true;
    }
    CheckResultInfo info;
    uint32_t setInfoNum = 0;
    for (size_t i = 0; i < children->size(); ++i) {
        (*children)[i]->accept(this);
        if (_checkResult.errorState) {
            if (setInfoNum == 0) {
                info = _checkResult;
            }
            ++setInfoNum;
        }
        _checkResult.reset();
    }
    if (setInfoNum > 0) {
        if (_isInANDNOTPart || setInfoNum == children->size()) {
            _checkResult = info;
            return false;
        }
    }
    return true;
}


} // namespace qrs
} // namespace isearch
