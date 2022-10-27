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
#include <ha3/turing/variant/AggregateResultsVariant.h>
#include <ha3/search/SearcherCacheInfo.h>
#include <ha3/rank/RankProfileManager.h>
#include <ha3/search/MatchDocSearcher.h>
#include <ha3/search/SearchCommonResource.h>
#include <ha3/search/InnerSearchResult.h>
#include <ha3/service/SearcherResource.h>
#include <indexlib/partition/index_partition.h>
#include <ha3/monitor/SessionMetricsCollector.h>
#include <autil/mem_pool/Pool.h>
#include <ha3/proto/ProtoClassUtil.h>
#include <matchdoc/MatchDocAllocator.h>
#include <ha3/common/HashJoinInfo.h>

BEGIN_HA3_NAMESPACE(turing);

class Ha3SeekAndJoinOp : public tensorflow::OpKernel
{
public:
    explicit Ha3SeekAndJoinOp(tensorflow::OpKernelConstruction* ctx);

public:
    void Compute(tensorflow::OpKernelContext* ctx) override;

private:
    common::HashJoinInfoPtr buildJoinHashInfo(
            const matchdoc::MatchDocAllocator *matchDocAllocator,
            const std::vector<matchdoc::MatchDoc> &matchDocs);
    void seek(tensorflow::OpKernelContext* ctx,
              common::Request *request,
              const rank::RankProfile *rankProfile,
              autil::mem_pool::Pool *pool,
              SearcherSessionResource *searcherSessionResource,
              SearcherQueryResource *searcherQueryResource,
              service::SearcherResource *searcherResource,
              common::HashJoinInfo *hashJoinInfo);
    static bool processRequest(common::Request *request,
                               const rank::RankProfile *rankProfile,
                               SearcherQueryResource *searcherQueryResource,
                               service::SearcherResource *searcherResource,
                               common::HashJoinInfo *hashJoinInfo,
                               search::InnerSearchResult &innerResult);
private:
    std::string _auxTableName;
    std::string _joinFieldName;

private:
    HA3_LOG_DECLARE();

};

END_HA3_NAMESPACE(turing);
