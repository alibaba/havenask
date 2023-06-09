/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once
#include "autil/Log.h" // IWYU pragma: keep
#include "indexlib/partition/index_partition.h"
#include "suez/turing/common/SessionResource.h"
#include "tensorflow/core/framework/op_kernel.h"

#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/proto/ProtoClassUtil.h"
#include "ha3/rank/RankProfileManager.h"
#include "ha3/search/MatchDocSearcher.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/search/SearcherCacheInfo.h"
#include "ha3/service/SearcherResource.h"
#include "ha3/turing/ops/SearcherQueryResource.h"
#include "ha3/turing/ops/SearcherSessionResource.h"
#include "ha3/turing/variant/AggregateResultsVariant.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "ha3/turing/variant/Ha3ResultVariant.h"
#include "ha3/turing/variant/InnerResultVariant.h"
#include "ha3/turing/variant/SeekResourceVariant.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace turing {

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
    AUTIL_LOG_DECLARE();

};

} // namespace turing
} // namespace isearch
