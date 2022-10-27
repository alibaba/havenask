#ifndef ISEARCH_QUERYVISITOR_H
#define ISEARCH_QUERYVISITOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/TermQuery.h>
#include <ha3/common/PhraseQuery.h>
#include <ha3/common/AndQuery.h>
#include <ha3/common/OrQuery.h>
#include <ha3/common/AndNotQuery.h>
#include <ha3/common/RankQuery.h>
#include <ha3/common/NumberQuery.h>
#include <ha3/common/MultiTermQuery.h>
#include <ha3/common/TableQuery.h>

BEGIN_HA3_NAMESPACE(common);

class QueryVisitor
{
public:
    virtual void visitTermQuery(const TermQuery *query) = 0;
    virtual void visitPhraseQuery(const PhraseQuery *query) = 0;
    virtual void visitAndQuery(const AndQuery *query) = 0;
    virtual void visitOrQuery(const OrQuery *query) = 0;
    virtual void visitAndNotQuery(const AndNotQuery *query) = 0;
    virtual void visitRankQuery(const RankQuery *query) = 0;
    virtual void visitNumberQuery(const NumberQuery *query) = 0;
    virtual void visitMultiTermQuery(const MultiTermQuery *query) = 0;
    virtual void visitTableQuery(const TableQuery *query) {
    }
    virtual ~QueryVisitor() {}
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(common);

#endif //ISEARCH_QUERYVISITOR_H
