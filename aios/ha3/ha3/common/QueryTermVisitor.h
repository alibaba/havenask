#ifndef ISEARCH_QUERYTERMVISITOR_H
#define ISEARCH_QUERYTERMVISITOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/QueryVisitor.h>
#include <ha3/common/Query.h>
#include <ha3/common/DocIdClause.h>

BEGIN_HA3_NAMESPACE(common);

class QueryTermVisitor : public QueryVisitor
{
public:
    typedef int32_t VisitTermType;
    static const VisitTermType VT_RANK_QUERY = 1;
    static const VisitTermType VT_ANDNOT_QUERY = 2;
    static const VisitTermType VT_NO_AUX_TERM = 0;
    static const VisitTermType VT_ALL = 0xffff;
public:
    QueryTermVisitor(VisitTermType visitType = VT_RANK_QUERY);
    virtual ~QueryTermVisitor();
public:
    virtual void visitTermQuery(const TermQuery *query);
    virtual void visitPhraseQuery(const PhraseQuery *query);
    virtual void visitAndQuery(const AndQuery *query);
    virtual void visitOrQuery(const OrQuery *query);
    virtual void visitAndNotQuery(const AndNotQuery *query);
    virtual void visitRankQuery(const RankQuery *query);
    virtual void visitNumberQuery(const NumberQuery *query);
    virtual void visitMultiTermQuery(const MultiTermQuery *query);
    const TermVector& getTermVector() const {
        return _termVector;
    }
private:
    void visitAdvancedQuery(const Query *query);
private:
    TermVector _termVector;
    VisitTermType _visitTermType;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(QueryTermVisitor);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_QUERYTERMVISITOR_H
