#ifndef ISEARCH_CHECKMATCHVISITOR_H
#define ISEARCH_CHECKMATCHVISITOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/QueryVisitor.h>
#include <ha3/common/Query.h>
#include <ha3/common/DocIdClause.h>

BEGIN_HA3_NAMESPACE(qrs);
enum MATCH_CHECK_ERROR{
    MCE_TOKENIZE = 0, MCE_NOT_QUERY, MCE_PHRASE_QUERY, MCE_INDEX_QUERY, MCE_UNKNOWN
};

struct CheckResultInfo{
    CheckResultInfo(){
        errorState = false;
        errorType = MCE_UNKNOWN;
    }
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
    CheckMatchVisitor(std::map<std::string, bool>& matchInfoMap);
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
private:
    std::map<std::string, bool> _matchInfoMap;
    std::map<std::string, std::string> _phraseQueryMap;
    CheckResultInfo _checkResult;
    bool _isInANDNOTPart;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(CheckMatchVisitor);

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_QUERYTERMVISITOR_H
