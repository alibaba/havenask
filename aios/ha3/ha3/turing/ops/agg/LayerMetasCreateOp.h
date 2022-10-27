#pragma once
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <ha3/turing/variant/LayerMetasVariant.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <basic_ops/util/OpUtil.h>
#include <indexlib/partition/index_partition.h>

BEGIN_HA3_NAMESPACE(turing);

class LayerMetasCreateOp : public tensorflow::OpKernel
{
public:
    explicit LayerMetasCreateOp(tensorflow::OpKernelConstruction* ctx)
        : tensorflow::OpKernel(ctx)
    {}
public:
    void Compute(tensorflow::OpKernelContext* ctx) override;
public:
    static search::LayerMetasPtr createLayerMetas(
            IE_NAMESPACE(partition)::PartitionReaderSnapshot *indexSnapshot,
            const std::string &mainTableName,
            autil::mem_pool::Pool *pool,
            common::QueryLayerClause* queryLayerClause);
    static void updateQuota(search::LayerMetasPtr &layerMetas);

private:
    HA3_LOG_DECLARE();
};
END_HA3_NAMESPACE(turing);
