#pragma once
#include <autil/Log.h>
#include <tensorflow/core/framework/op_kernel.h>
#include "ha3/common/ha3_op_common.h"
#include "suez/turing/common/SessionResource.h"
#include <ha3/util/Log.h>
#include <ha3/turing/ops/SearcherQueryResource.h>
#include <ha3/turing/ops/SearcherSessionResource.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/turing/variant/Ha3ResultVariant.h>
#include <ha3/turing/variant/SeekResourceVariant.h>
#include <ha3/turing/variant/AggregateResultsVariant.h>
#include <ha3/search/SearcherCacheInfo.h>
#include <ha3/rank/RankProfileManager.h>
#include <ha3/search/MatchDocSearcher.h>
#include <ha3/search/SearchCommonResource.h>
#include <ha3/turing/variant/InnerResultVariant.h>
#include <ha3/service/SearcherResource.h>
#include <indexlib/partition/index_partition.h>
#include <ha3/monitor/SessionMetricsCollector.h>
#include <ha3/proto/ProtoClassUtil.h>

BEGIN_HA3_NAMESPACE(turing);

class Ha3SeekParaOp : public tensorflow::OpKernel
{
public:
    explicit Ha3SeekParaOp(tensorflow::OpKernelConstruction* ctx)
        : tensorflow::OpKernel(ctx)
    {}
public:
    void Compute(tensorflow::OpKernelContext* ctx) override;

private:
    void seek(tensorflow::OpKernelContext* ctx,
              common::Request *request,
              const rank::RankProfile *rankProfile,
              SeekResource *seekResource,
              service::SearcherResource *searcherResource);

    static bool processRequest(common::Request *request,
                               const rank::RankProfile *rankProfile,
                               SeekResource *seekResource,
                               service::SearcherResource *searcherResource,
                               search::InnerSearchResult &innerResult);

private:
    HA3_LOG_DECLARE();

};

END_HA3_NAMESPACE(turing);
