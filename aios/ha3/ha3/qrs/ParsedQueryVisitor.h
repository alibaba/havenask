#ifndef ISEARCH_PARSEDQUERYVISITOR_H
#define ISEARCH_PARSEDQUERYVISITOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/QueryVisitor.h>
#include <ha3/common/Query.h>
#include <ha3/common/DocIdClause.h>

BEGIN_HA3_NAMESPACE(qrs);

class ParsedQueryVisitor : public common::QueryVisitor
{
public:
    ParsedQueryVisitor();
    virtual ~ParsedQueryVisitor();
public:
    virtual void visitTermQuery(const common::TermQuery *query);
    virtual void visitPhraseQuery(const common::PhraseQuery *query);
    virtual void visitAndQuery(const common::AndQuery *query);
    virtual void visitOrQuery(const common::OrQuery *query);
    virtual void visitAndNotQuery(const common::AndNotQuery *query);
    virtual void visitRankQuery(const common::RankQuery *query);
    virtual void visitNumberQuery(const common::NumberQuery *query);
    virtual void visitMultiTermQuery(const common::MultiTermQuery *query);

    const std::string& getParsedQueryStr() const {
        return _queryStr;
    }
private:
    void visitAdvancedQuery(const common::Query *query, const std::string& prefix);
    void addTerm(const common::Term& term, std::string& str);
    void visitTermArray(const std::vector<common::TermPtr> &termArray, 
                        std::string &str);
private:
    std::string _queryStr;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(ParsedQueryVisitor);

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_QUERYTERMVISITOR_H
