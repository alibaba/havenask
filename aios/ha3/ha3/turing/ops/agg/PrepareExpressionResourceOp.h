#ifndef ISEARCH_PREPAREEXPRESSIONRESOURCEOP_H
#define ISEARCH_PREPAREEXPRESSIONRESOURCEOP_H
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <ha3/common/Request.h>
#include <basic_ops/util/OpUtil.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <suez/turing/expression/function/FunctionInterfaceCreator.h>

BEGIN_HA3_NAMESPACE(turing);

class PrepareExpressionResourceOp : public tensorflow::OpKernel
{
public:
    explicit PrepareExpressionResourceOp(tensorflow::OpKernelConstruction* ctx)
        : tensorflow::OpKernel(ctx)
    {}
public:
    void Compute(tensorflow::OpKernelContext* ctx) override;
public:
    static ExpressionResourcePtr createExpressionResource(
            const common::RequestPtr &request,
            const suez::turing::TableInfoPtr &tableInfoPtr,
            const std::string &mainTableName,
            const suez::turing::FunctionInterfaceCreatorPtr &functionInterfaceCreator,
            IE_NAMESPACE(partition)::PartitionReaderSnapshot *partitionReaderSnapshot,
            suez::turing::Tracer *tracer,
            const suez::turing::CavaPluginManagerPtr &cavaPluginManager,
            autil::mem_pool::Pool *pool,
            suez::turing::SuezCavaAllocator *cavaAllocator);

private:
    static bool useSubDoc(const common::Request *request,
                          const suez::turing::TableInfo *tableInfo);
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(turing);
#endif //ISEARCH_PREPAREEXPRESSIONRESOURCEOP_H
