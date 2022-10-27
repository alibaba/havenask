#pragma once
#include <autil/Log.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <iostream>
#include <ha3/common/ha3_op_common.h>
#include <suez/turing/common/SessionResource.h>
#include <ha3/util/Log.h>
#include <ha3/turing/ops/SearcherQueryResource.h>
#include <ha3/turing/ops/SearcherSessionResource.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/turing/variant/Ha3ResultVariant.h>
#include <ha3/search/SummarySearcher.h>
#include <ha3/search/SearchCommonResource.h>
#include <ha3/service/SearcherResource.h>
#include <indexlib/partition/index_partition.h>
#include <ha3/monitor/SessionMetricsCollector.h>
#include <ha3/common/Tracer.h>
#include <ha3/common/TimeoutTerminator.h>
#include <autil/mem_pool/Pool.h>
#include <ha3/proto/ProtoClassUtil.h>

BEGIN_HA3_NAMESPACE(turing);

class Ha3ExtraSummaryOp : public tensorflow::OpKernel
{
public:
    explicit Ha3ExtraSummaryOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
        OP_REQUIRES_OK(ctx, ctx->GetAttr("table_name", &_tableName));

        auto sessionResource = GET_SESSION_RESOURCE();
        auto *searcherSessionResource =
            dynamic_cast<SearcherSessionResource *>(sessionResource);
        OP_REQUIRES(ctx, searcherSessionResource, tensorflow::errors::Unavailable("get searcher session resource failed"));
        auto searcherResource = searcherSessionResource->searcherResource;
        OP_REQUIRES(ctx, searcherResource, tensorflow::errors::Unavailable("get searcher resource failed"));

        if (_tableName.empty()) {
            _tableName = searcherResource->getClusterConfig().getTableName() + "_extra";
        }

        auto tableInfo = sessionResource->dependencyTableInfoMap.find(_tableName);
        if (tableInfo == sessionResource->dependencyTableInfoMap.end()) {
            OP_REQUIRES(ctx, false, tensorflow::errors::Unavailable("find extra table info failed"));
        }

        config::HitSummarySchemaPtr hitSummarySchemaPtr(new config::HitSummarySchema(tableInfo->second.get()));
        config::HitSummarySchemaPtr mainHitSummarySchemaPtr = searcherResource->getHitSummarySchema();
        _schemaConsistent = (hitSummarySchemaPtr->getSignature()
                             == mainHitSummarySchemaPtr->getSignature());

    }
public:
    void Compute(tensorflow::OpKernelContext* ctx) override;
private:
    common::ResultPtr supply(const common::Request *request,
                             const common::ResultPtr& inputResult,
                             service::SearcherResourcePtr resourcePtr,
                             SearcherQueryResource *queryResource,
                             const suez::turing::TableInfoPtr& tableInfo);
    void addTraceInfo(monitor::SessionMetricsCollector *metricsCollector, suez::turing::Tracer* tracer);
    bool needExtraSearch(common::ResultPtr inputResult);
    std::string _tableName;
    bool _schemaConsistent = false;

private:
    HA3_LOG_DECLARE();

};
END_HA3_NAMESPACE(turing);
