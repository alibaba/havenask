#ifndef ISEARCH_REWRITE_QUERYTERMVISITOR_H
#define ISEARCH_REWRITE_QUERYTERMVISITOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Query.h>
#include <ha3/common/DocIdClause.h>
#include <ha3/common/QueryTermVisitor.h>

BEGIN_HA3_NAMESPACE(common);
typedef std::map<std::string, std::string> TokenRewriteMap;

class RewriteQueryTermVisitor : public QueryTermVisitor
{
public:
    RewriteQueryTermVisitor();
    RewriteQueryTermVisitor(const TokenRewriteMap& tokenRewriteMap);
    virtual ~RewriteQueryTermVisitor();
public:
    virtual void visitTermQuery(const TermQuery *query);
    virtual void visitPhraseQuery(const PhraseQuery *query);
    virtual void visitAndQuery(const AndQuery *query);
    virtual void visitOrQuery(const OrQuery *query);
    virtual void visitAndNotQuery(const AndNotQuery *query);
    virtual void visitRankQuery(const RankQuery *query);
    virtual void visitNumberQuery(const NumberQuery *query);
    virtual void visitMultiTermQuery(const MultiTermQuery *query);

    bool rewriteQueryTerm();

    TermVector& getTermVector() { return _termVector; }

    void rewriteTerm(Term &term);
    void visitAdvancedQuery(const Query *query);
private:
    TermVector _termVector;
    TokenRewriteMap _tokenRewriteMap;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(RewriteQueryTermVisitor);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_QUERYTERMVISITOR_H
