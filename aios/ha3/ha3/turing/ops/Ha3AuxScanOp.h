#ifndef ISEARCH_HA3AUXSCANOP_H
#define ISEARCH_HA3AUXSCANOP_H

#include <string>

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/FilterClause.h>
#include <ha3/search/FilterWrapper.h>
#include <ha3/search/LayerMetas.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <suez/turing/expression/framework/AttributeExpressionCreator.h>
#include <matchdoc/MatchDocAllocator.h>

BEGIN_HA3_NAMESPACE(turing);

class Ha3AuxScanOp : public tensorflow::OpKernel
{
public:
    explicit Ha3AuxScanOp(tensorflow::OpKernelConstruction* ctx);
    ~Ha3AuxScanOp();
private:
    Ha3AuxScanOp(const Ha3AuxScanOp &);
    Ha3AuxScanOp& operator=(const Ha3AuxScanOp &);
public:
    void Compute(tensorflow::OpKernelContext* ctx) override;
private:
    HA3_NS(search)::FilterWrapperPtr createFilterWrapper(
        const HA3_NS(common)::AuxFilterClause *auxFilterClause,
        suez::turing::AttributeExpressionCreator *attrExprCreator,
        matchdoc::MatchDocAllocator *matchDocAllocator,
	autil::mem_pool::Pool *pool);

    HA3_NS(search)::LayerMetaPtr createLayerMeta(
        HA3_NS(search)::IndexPartitionReaderWrapperPtr &indexPartitionReader,
        autil::mem_pool::Pool *pool);

private:
    std::string _tableName;
    std::string _joinFieldName;
    kmonitor::MetricsTags _tags;

private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(Ha3AuxScanOp);

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_HA3AUXSCANOP_H
