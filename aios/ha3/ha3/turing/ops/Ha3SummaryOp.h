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
#include <iostream>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/mem_pool/Pool.h"
#include "indexlib/partition/index_partition.h"
#include "suez/turing/common/SessionResource.h"
#include "tensorflow/core/framework/op_kernel.h"

#include "ha3/common/TimeoutTerminator.h"
#include "ha3/common/Tracer.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/proto/ProtoClassUtil.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/search/SummarySearcher.h"
#include "ha3/service/SearcherResource.h"
#include "ha3/turing/ops/SearcherQueryResource.h"
#include "ha3/turing/ops/SearcherSessionResource.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "ha3/turing/variant/Ha3ResultVariant.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace turing {

class Ha3SummaryOp : public tensorflow::OpKernel
{
public:
    explicit Ha3SummaryOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
        OP_REQUIRES_OK(ctx, ctx->GetAttr("force_allow_lack_of_summary", &_forceAllowLackOfSummary));
    }
public:
    void Compute(tensorflow::OpKernelContext* ctx) override;

    common::ResultPtr processPhaseTwoRequest(const common::Request *request,
            service::SearcherResourcePtr resourcePtr,
            SearcherSessionResource *searcherSessionResource,
            SearcherQueryResource *queryResource);
    void addTraceInfo(monitor::SessionMetricsCollector *metricsCollector,
                      suez::turing::Tracer* tracer);
private:
    bool _forceAllowLackOfSummary = false;
private:
    AUTIL_LOG_DECLARE();

};
} // namespace turing
} // namespace isearch
