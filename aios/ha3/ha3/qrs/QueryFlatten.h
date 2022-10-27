#ifndef ISEARCH_QUERYFLATTEN_H
#define ISEARCH_QUERYFLATTEN_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/QueryVisitor.h>
#include <ha3/common/Query.h>

BEGIN_HA3_NAMESPACE(qrs);

class QueryFlatten : public common::QueryVisitor
{
public:
    QueryFlatten();
    virtual ~QueryFlatten();
public:
    void flatten(common::Query* query) {
        DELETE_AND_SET_NULL(_visitedQuery);
        query->accept(this);
    }

    virtual void visitTermQuery(const common::TermQuery *query);
    virtual void visitPhraseQuery(const common::PhraseQuery *query);
    virtual void visitAndQuery(const common::AndQuery *query);
    virtual void visitOrQuery(const common::OrQuery *query);
    virtual void visitAndNotQuery(const common::AndNotQuery *query);
    virtual void visitRankQuery(const common::RankQuery *query);
    virtual void visitNumberQuery(const common::NumberQuery *query);
    virtual void visitMultiTermQuery(const common::MultiTermQuery *query);

    common::Query* stealQuery();
private:
    void flatten(common::Query *resultQuery, const common::Query *srcQuery);
private:
    common::Query *_visitedQuery;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(QueryFlatten);

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_QUERYFLATTEN_H
