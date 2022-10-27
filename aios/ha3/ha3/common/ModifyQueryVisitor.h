#ifndef ISEARCH_MODIFYQUERYVISITOR_H
#define ISEARCH_MODIFYQUERYVISITOR_H

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

class ModifyQueryVisitor
{
public:
    virtual void visitTermQuery(TermQuery *query) = 0;
    virtual void visitPhraseQuery(PhraseQuery *query) = 0;
    virtual void visitAndQuery(AndQuery *query) = 0;
    virtual void visitOrQuery(OrQuery *query) = 0;
    virtual void visitAndNotQuery(AndNotQuery *query) = 0;
    virtual void visitRankQuery(RankQuery *query) = 0;
    virtual void visitNumberQuery(NumberQuery *query) = 0;
    virtual void visitMultiTermQuery(MultiTermQuery *query) = 0;
    virtual void visitTableQuery(TableQuery *query) {
    }
    virtual ~ModifyQueryVisitor() {}
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(common);

#endif //ISEARCH_MODIFYQUERYVISITOR_H
