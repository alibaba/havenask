#ifndef ISEARCH_TERMDFVISITOR_H
#define ISEARCH_TERMDFVISITOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/QueryVisitor.h>
#include <ha3/search/AuxiliaryChainDefine.h>

BEGIN_HA3_NAMESPACE(search);

class TermDFVisitor : public common::QueryVisitor
{
public:
    TermDFVisitor(const TermDFMap &auxListTerm);
    ~TermDFVisitor();
private:
    TermDFVisitor(const TermDFVisitor &);
    TermDFVisitor& operator=(const TermDFVisitor &);
public:
    void visitTermQuery(const common::TermQuery *query) override;
    void visitPhraseQuery(const common::PhraseQuery *query) override;
    void visitAndQuery(const common::AndQuery *query) override;
    void visitOrQuery(const common::OrQuery *query) override;
    void visitAndNotQuery(const common::AndNotQuery *query) override;
    void visitRankQuery(const common::RankQuery *query) override;
    void visitNumberQuery(const common::NumberQuery *query) override;
    void visitMultiTermQuery(const common::MultiTermQuery *query) override;
public:
    df_t getDF() const {
        return _df;
    }
private:
    const TermDFMap &_auxListTerm;
    df_t _df;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(TermDFVisitor);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_TERMDFVISITOR_H
