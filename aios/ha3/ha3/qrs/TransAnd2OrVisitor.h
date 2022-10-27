#ifndef ISEARCH_TRANSANDTOORVISITOR_H
#define ISEARCH_TRANSANDTOORVISITOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/QueryVisitor.h>
#include <ha3/common/Query.h>
#include <ha3/common/DocIdClause.h>

BEGIN_HA3_NAMESPACE(qrs);

class TransAnd2OrVisitor : public common::QueryVisitor
{
public:
    TransAnd2OrVisitor();
    virtual ~TransAnd2OrVisitor();
public:
    virtual void visitTermQuery(const common::TermQuery *query);
    virtual void visitPhraseQuery(const common::PhraseQuery *query);
    virtual void visitAndQuery(const common::AndQuery *query);
    virtual void visitOrQuery(const common::OrQuery *query);
    virtual void visitAndNotQuery(const common::AndNotQuery *query);
    virtual void visitRankQuery(const common::RankQuery *query);
    virtual void visitNumberQuery(const common::NumberQuery *query);
    virtual void visitMultiTermQuery(const common::MultiTermQuery *query);

    common::Query *stealQuery();

private:
    void visitAdvancedQuery(const common::Query *query);
private:
    common::Query *_query;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(TransAnd2OrVisitor);

END_HA3_NAMESPACE(qrs);

#endif 
