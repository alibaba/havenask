#ifndef ISEARCH_QUERYVALIDATOR_H
#define ISEARCH_QUERYVALIDATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/QueryVisitor.h>
#include <ha3/common/Query.h>
#include <suez/turing/expression/util/IndexInfos.h>
#include <ha3/common/ErrorDefine.h>

BEGIN_HA3_NAMESPACE(qrs);

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
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(QueryValidator);

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_QUERYVALIDATOR_H
