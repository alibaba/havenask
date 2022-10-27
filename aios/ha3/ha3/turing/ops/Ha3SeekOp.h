#pragma once
#include <autil/Log.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <iostream>
#include <ha3/common/ha3_op_common.h>
#include <suez/turing/common/SessionResource.h>
#include <ha3/util/Log.h>
#include <ha3/turing/ops/SearcherQueryResource.h>
#include <ha3/turing/ops/SearcherSessionResource.h>
#include <ha3/search/SearcherCacheInfo.h>
#include <ha3/rank/RankProfileManager.h>
#include <ha3/search/MatchDocSearcher.h>
#include <ha3/search/SearchCommonResource.h>
#include <autil/mem_pool/Pool.h>
#include <ha3/proto/ProtoClassUtil.h>

BEGIN_HA3_NAMESPACE(turing);

class Ha3SeekOp : public tensorflow::OpKernel
{
public:
    explicit Ha3SeekOp(tensorflow::OpKernelConstruction* ctx)
        : tensorflow::OpKernel(ctx)
    {}
public:
    void Compute(tensorflow::OpKernelContext* ctx) override;
    static bool processRequest(common::Request *request,
                               const rank::RankProfile *rankProfile,
                               SearcherQueryResource *searcherQueryResource,
                               service::SearcherResource *searcherResource,
                               search::InnerSearchResult &innerResult);
    static common::ResultPtr constructErrorResult(common::Request *request,
            SearcherSessionResource *searcherSessionResource,
            SearcherQueryResource *searcherQueryResource,
            service::SearcherResource *searcherResource,
            search::SearchCommonResource *commonResource);
    static void outputResult(tensorflow::OpKernelContext* ctx,
                             bool ret,
                             search::InnerSearchResult &innerResult,
                             common::Request *request,
                             autil::mem_pool::Pool *pool,
                             SearcherSessionResource *searcherSessionResource,
                             SearcherQueryResource *searcherQueryResource,
                             service::SearcherResource *searcherResource);
private:
    void seek(tensorflow::OpKernelContext* ctx,
                     common::Request *request,
                     const rank::RankProfile *rankProfile,
                     autil::mem_pool::Pool *pool,
                     SearcherSessionResource *searcherSessionResource,
                     SearcherQueryResource *searcherQueryResource,
                     service::SearcherResource *searcherResource);


private:
    HA3_LOG_DECLARE();

};

END_HA3_NAMESPACE(turing);
