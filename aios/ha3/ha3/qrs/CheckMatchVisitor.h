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

#include <map>
#include <memory>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/QueryVisitor.h"

namespace isearch {
namespace common {
class AndNotQuery;
class AndQuery;
class MultiTermQuery;
class NumberQuery;
class OrQuery;
class PhraseQuery;
class Query;
class RankQuery;
class Term;
class TermQuery;
}  // namespace common
}  // namespace isearch

namespace isearch {
namespace qrs {
enum MATCH_CHECK_ERROR{
    MCE_TOKENIZE = 0, MCE_NOT_QUERY, MCE_PHRASE_QUERY, MCE_INDEX_QUERY, MCE_UNKNOWN
};

struct CheckResultInfo{
    CheckResultInfo() : errorState(false), errorType(MCE_UNKNOWN) {}
    void reset(){
        errorState = false;
        errorType = MCE_UNKNOWN;
        indexStr.clear();
        wordStr.clear();
    }
    static  std::string getErrorType(MATCH_CHECK_ERROR error){
        switch(error){
        case MCE_TOKENIZE:
            return "TOKENIZE";
        case MCE_NOT_QUERY:
            return "NOT";
        case MCE_INDEX_QUERY:
            return "INDEX";
        case MCE_PHRASE_QUERY:
            return "PHRASE";
        case MCE_UNKNOWN:
            return "UNKNOWN";
        default:
            return "UNKNOWN";
        }
    }
    static  MATCH_CHECK_ERROR getErrorType(const std::string& type){
        if(type == "TOKENIZE"){
            return MCE_TOKENIZE;
        }else if (type == "NOT"){
            return MCE_NOT_QUERY;
        }else if (type == "INDEX"){
            return MCE_INDEX_QUERY;
        }else if (type == "PHRASE"){
            return MCE_PHRASE_QUERY;
        }else {
            return MCE_UNKNOWN;
        }
    }
    bool operator == ( const CheckResultInfo& rha){
        return errorState == rha.errorState && errorType == rha.errorType 
            && indexStr == rha.indexStr && wordStr == rha.wordStr;
    }
    bool errorState;
    MATCH_CHECK_ERROR errorType;
    std::string indexStr;
    std::string wordStr;
};

class CheckMatchVisitor : public common::QueryVisitor
{
public:
    CheckMatchVisitor(const std::string &traceInfo);
    virtual ~CheckMatchVisitor();
public:
    virtual void visitTermQuery(const common::TermQuery *query);
    virtual void visitPhraseQuery(const common::PhraseQuery *query);
    virtual void visitAndQuery(const common::AndQuery *query);
    virtual void visitOrQuery(const common::OrQuery *query);
    virtual void visitAndNotQuery(const common::AndNotQuery *query);
    virtual void visitRankQuery(const common::RankQuery *query);
    virtual void visitNumberQuery(const common::NumberQuery *query);
    virtual void visitMultiTermQuery(const common::MultiTermQuery *query);
    const CheckResultInfo& getCheckResult() const { return _checkResult; }
    const std::map<std::string, std::string>& getPhraseQuerys() const{ return _phraseQueryMap;}
private:
    bool checkAtLeastOneMatch(const common::Query *query);
    bool checkTermMatch(const common::Term& term);
    void parseMatchInfo(const std::string &traceInfoStr);
private:
    std::map<std::string, bool> _matchInfoMap;
    std::map<std::string, std::string> _phraseQueryMap;
    CheckResultInfo _checkResult;
    bool _isInANDNOTPart;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<CheckMatchVisitor> CheckMatchVisitorPtr;

} // namespace qrs
} // namespace isearch

