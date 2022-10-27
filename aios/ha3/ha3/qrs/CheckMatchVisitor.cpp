#include <ha3/qrs/CheckMatchVisitor.h>
#include <ha3/common/Query.h>
#include <autil/StringUtil.h>
using namespace std;
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(qrs);
HA3_LOG_SETUP(qrs, CheckMatchVisitor);

CheckMatchVisitor::CheckMatchVisitor(
        map<string, bool>& matchInfoMap)
    
{
    _matchInfoMap = matchInfoMap;
    _isInANDNOTPart = false;
}

CheckMatchVisitor::~CheckMatchVisitor() {
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
        } else if (!info.errorState) {
            info = _checkResult;
        }
        if ( ((match && op == OP_OR)) ) {
            _checkResult.reset();
            break;
        }
        if ( (!match && (op == OP_AND)) ) {
            break;
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
    string keyStr = term.getIndexName() +":"+ term.getWord().c_str();
    map<string, bool>::const_iterator iter = _matchInfoMap.find(keyStr);
    if(((iter == _matchInfoMap.end() || !iter->second) && !_isInANDNOTPart)
       ||(iter != _matchInfoMap.end() && iter->second &&_isInANDNOTPart)
       ) { 
        _checkResult.errorState = true;
        if(!_isInANDNOTPart){
            _checkResult.errorType = MCE_TOKENIZE;
        }else{
            _checkResult.errorType = MCE_NOT_QUERY;
        }
        _checkResult.wordStr = term.getWord().c_str();
        _checkResult.indexStr = term.getIndexName();
        return false;
    }
    return true;
}

bool CheckMatchVisitor::checkAtLeastOneMatch(const Query *query) {
    const vector<QueryPtr>* children = query->getChildQuery();
    assert(children);
    if (children->size() == 0) {
        return true;
    }
    vector<CheckResultInfo> infoVec;
    for (size_t i = 0; i < children->size(); ++i) {
        (*children)[i]->accept(this);
        if(_checkResult.errorState){
            infoVec.push_back(_checkResult);
        }
        _checkResult.reset();
    }
    if(infoVec.size() > 0){ 
        if(_isInANDNOTPart){
            _checkResult = infoVec[0];
            return false;
        }else if( infoVec.size() == children->size()){
            _checkResult = infoVec[0];
            return false;
        }
    }
    return true;
}


END_HA3_NAMESPACE(qrs);

