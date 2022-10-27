#ifndef ISEARCH_AUXILIARYCHAINOPTIMIZER_H
#define ISEARCH_AUXILIARYCHAINOPTIMIZER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/Optimizer.h>
#include <ha3/search/AuxiliaryChainDefine.h>
#include <ha3/common/DocIdClause.h>

BEGIN_HA3_NAMESPACE(search);

class AuxiliaryChainOptimizer : public Optimizer
{
public:
    AuxiliaryChainOptimizer();
    ~AuxiliaryChainOptimizer();
public:
    bool init(OptimizeInitParam *param) override;
    std::string getName() const override {
        return "AuxChain";
    }
    OptimizerPtr clone() override;
    bool optimize(OptimizeParam *param) override;
    void disableTruncate() override;
private:
    void getTermDFs(const common::TermVector &termVector,
                    IndexPartitionReaderWrapper *readerWrapper,
                    TermDFMap &termDFMap);
    void insertNewQuery(const TermDFMap &termDFMap, common::QueryClause *queryClause);
public:
    // for test
    bool isActive() const { return _active; }
private:
    static QueryInsertType covertQueryInsertType(const std::string &str);
    static SelectAuxChainType covertSelectAuxChainType(const std::string &str);
private:
    QueryInsertType _queryInsertType;
    SelectAuxChainType _selectAuxChainType;
    std::string _auxChainName;
    bool _active;
private:
    friend class AuxiliaryChainOptimizerTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(AuxiliaryChainOptimizer);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_AUXILIARYCHAINOPTIMIZER_H
