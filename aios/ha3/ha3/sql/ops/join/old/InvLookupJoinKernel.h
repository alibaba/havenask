#ifndef ISEARCH_INV_LOOKUP_JOIN_KERNEL_H
#define ISEARCH_INV_LOOKUP_JOIN_KERNEL_H

#include <ha3/sql/common/common.h>
#include <ha3/sql/data/Table.h>
#include <ha3/sql/ops/join/LookupJoinKernel.h>
#include <ha3/sql/ops/scan/ScanIteratorCreator.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <matchdoc/MatchDocAllocator.h>
#include <matchdoc/MatchDoc.h>
#include <suez/turing/expression/framework/AttributeExpressionCreator.h>
#include <suez/turing/expression/framework/ExpressionEvaluator.h>
#include <navi/engine/Kernel.h>
#include <navi/resource/MemoryPoolResource.h>

BEGIN_HA3_NAMESPACE(sql);

class ScanIterator;

class InvLookupJoinKernel : public LookupJoinKernel {
public:
    InvLookupJoinKernel();
    ~InvLookupJoinKernel();
public:
    const navi::KernelDef *getDef() const override;
    bool config(navi::KernelConfigContext &ctx) override;
private:
    navi::ErrorCode doInit(SqlBizResource *bizResource, SqlQueryResource *queryResource) override;
    bool initExpressionCreator(const suez::turing::TableInfoPtr &tableInfo,
                               bool useSub, SqlBizResource *bizResource,
                               SqlQueryResource *queryResource);
    bool initScanColumn();
    bool singleJoin(ColumnPtr joinColumn, ValueType vt, size_t rowCount) override;
    bool multiJoin(ColumnPtr joinColumn, ValueType vt, size_t rowCount) override;
    bool seekAndJoin(const std::string &joinValue, size_t leftIndex);
    ScanIterator *createScanIter(const std::string &joinValue, bool &emptyScan);
    TablePtr createTable(const std::vector<matchdoc::MatchDoc> &matchDocVec,
                         const matchdoc::MatchDocAllocatorPtr &matchDocAllocator);
private:
    std::string _invTableName;
    std::map<std::string, IndexInfo> _invTableIndexes;
    bool _useSub;

    matchdoc::MatchDocAllocatorPtr _matchDocAllocator;
    ScanIteratorCreatorParam _param;
    suez::turing::FunctionProviderPtr _functionProvider;
    search::IndexPartitionReaderWrapperPtr _indexPartitionReaderWrapper;
    suez::turing::AttributeExpressionCreatorPtr _attributeExpressionCreator;
    std::vector<suez::turing::AttributeExpression *> _attributeExpressionVec;
    std::shared_ptr<suez::turing::ExpressionEvaluator<
        std::vector<suez::turing::AttributeExpression *>>> _evaluator;
};

HA3_TYPEDEF_PTR(InvLookupJoinKernel);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_INV_LOOK_UP_JOIN_KERNEL_H
