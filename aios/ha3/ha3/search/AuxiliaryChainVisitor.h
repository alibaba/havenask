#ifndef ISEARCH_AUXILIARYCHAINVISITOR_H
#define ISEARCH_AUXILIARYCHAINVISITOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/ModifyQueryVisitor.h>
#include <ha3/search/AuxiliaryChainDefine.h>

BEGIN_HA3_NAMESPACE(search);

class AuxiliaryChainVisitor : public common::ModifyQueryVisitor
{
public:
    AuxiliaryChainVisitor(const std::string &auxChainName,
                          const TermDFMap &termDFMap,
                          SelectAuxChainType type);
    ~AuxiliaryChainVisitor();
private:
    AuxiliaryChainVisitor(const AuxiliaryChainVisitor &);
    AuxiliaryChainVisitor& operator=(const AuxiliaryChainVisitor &);
public:
    void visitTermQuery(common::TermQuery *query) override;
    void visitPhraseQuery(common::PhraseQuery *query) override;
    void visitAndQuery(common::AndQuery *query) override;
    void visitOrQuery(common::OrQuery *query) override;
    void visitAndNotQuery(common::AndNotQuery *query) override;
    void visitRankQuery(common::RankQuery *query) override;
    void visitNumberQuery(common::NumberQuery *query) override;
    void visitMultiTermQuery(common::MultiTermQuery *query) override;
private:
    std::string _auxChainName;
    const TermDFMap &_termDFMap;
    SelectAuxChainType _type;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(AuxiliaryChainVisitor);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_AUXILIARYCHAINVISITOR_H
