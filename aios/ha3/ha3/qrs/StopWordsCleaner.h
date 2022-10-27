#ifndef ISEARCH_STOPWORDSCLEANER_H
#define ISEARCH_STOPWORDSCLEANER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/QueryVisitor.h>
#include <ha3/common/ErrorDefine.h>

BEGIN_HA3_NAMESPACE(qrs);

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
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(StopWordsCleaner);

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_STOPWORDSCLEANER_H
