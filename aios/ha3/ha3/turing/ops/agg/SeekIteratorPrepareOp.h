#ifndef ISEARCH_SEEKITERATORPREPAREOP_H
#define ISEARCH_SEEKITERATORPREPAREOP_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <ha3/turing/variant/SeekIteratorVariant.h>
#include <ha3/search/QueryExecutor.h>
#include <ha3/search/FilterWrapper.h>
#include <ha3/search/LayerMetas.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <ha3/search/PKQueryExecutor.h>
#include <indexlib/partition/index_partition_reader.h>
#include <matchdoc/MatchDocAllocator.h>
#include <basic_ops/util/OpUtil.h>

BEGIN_HA3_NAMESPACE(turing);

class SeekIteratorPrepareOp : public tensorflow::OpKernel
{
public:
    explicit SeekIteratorPrepareOp(tensorflow::OpKernelConstruction* ctx)
        : tensorflow::OpKernel(ctx)
    {}
public:
    void Compute(tensorflow::OpKernelContext* ctx) override;
public:
    static bool createSeekIterator(const std::string &mainTableName,
                                   const ExpressionResourcePtr &resource,
                                   const search::LayerMetasPtr &layerMetas,
                                   tensorflow::QueryResource *queryResource,
                                   SeekIteratorPtr &seekIterator);
private:
    static bool createFilterWrapper(
            const common::FilterClause *filterClause,
            suez::turing::AttributeExpressionCreator *attrExprCreator,
            matchdoc::MatchDocAllocator *matchDocAllocator,
            autil::mem_pool::Pool *pool,
            search::FilterWrapperPtr &filterWrapper);
    static search::QueryExecutor* createQueryExecutor(
            const common::Query *query,
            search::IndexPartitionReaderWrapper *wrapper,
            const common::PKFilterClause *pkFilterClause,
            const search::LayerMeta *layerMeta,
            autil::mem_pool::Pool *pool,
            suez::turing::TimeoutTerminator *timeoutTerminator,
            const IE_NAMESPACE(partition)::IndexPartitionReaderPtr &indexPartReaderPtr);
    static search::QueryExecutor* createPKExecutor(
            const common::PKFilterClause *pkFilterClause,
            search::QueryExecutor *queryExecutor,
            const IE_NAMESPACE(partition)::IndexPartitionReaderPtr &indexPartReaderPtr,
            autil::mem_pool::Pool *pool);

private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(turing);
#endif //ISEARCH_SEEKITERATORPREPAREOP_H
