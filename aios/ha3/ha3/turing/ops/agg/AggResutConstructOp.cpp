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
#include <stddef.h>
#include <stdint.h>
#include <iostream>
#include <memory>
#include <string>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "autil/RangeUtil.h"
#include "autil/TimeUtility.h"
#include "ha3/common/AggregateResult.h"
#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/common/MatchDocs.h"
#include "ha3/common/Result.h"
#include "ha3/common/Tracer.h"
#include "ha3/common/searchinfo/PhaseOneSearchInfo.h"
#include "ha3/turing/ops/ha3_op_common.h"
#include "ha3/turing/variant/AggregateResultsVariant.h"
#include "ha3/turing/variant/Ha3ResultVariant.h"
#include "kmonitor/client/MetricType.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "suez/sdk/ServiceInfo.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "suez/turing/expression/common.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/op_kernel.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"
#include "tensorflow/core/platform/byte_order.h"
#include "turing_ops_util/util/OpUtil.h"

using namespace std;
using namespace autil;
using namespace tensorflow;
using namespace suez::turing;

using namespace isearch::common;
using namespace isearch::util;

namespace isearch {
namespace turing {

class AggResultConstructOp : public tensorflow::OpKernel
{
public:
    explicit AggResultConstructOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
        ADD_OP_BASE_TAG(_tags);
    }
public:

    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto sessionResource = GET_SESSION_RESOURCE();
        auto queryResource = GET_QUERY_RESOURCE(sessionResource);
        auto aggResultsTensor = ctx->input(0).scalar<Variant>()();
        auto aggResultsVariant = aggResultsTensor.get<AggregateResultsVariant>();
        OP_REQUIRES(ctx, aggResultsVariant, errors::Unavailable("get aggregate results variant failed"));
        AggregateResultsPtr aggResults = aggResultsVariant->getAggResults();
        uint64_t aggCount = aggResultsVariant->getAggCount();
        uint64_t matchCount = aggResultsVariant->getToAggCount();
        uint64_t seekedCount = aggResultsVariant->getSeekedCount();
        uint64_t seekTermDocCount = aggResultsVariant->getSeekTermDocCount();
        common::ResultPtr result(new common::Result());
        result->fillAggregateResults(aggResults);
        result->setTracer(queryResource->getTracerPtr());
        Ha3MatchDocAllocatorPtr allocator(new Ha3MatchDocAllocator(queryResource->getPool()));
        MatchDocs *matchDocs = new MatchDocs();
        matchDocs->setMatchDocAllocator(allocator);
        result->setMatchDocs(matchDocs);
        result->setTotalMatchDocs(matchCount);
        result->setActualMatchDocs(matchCount);
        PhaseOneSearchInfo *searchInfo = new PhaseOneSearchInfo;
        searchInfo->partitionCount = 1;
        searchInfo->matchCount = matchCount;
        searchInfo->seekCount = seekedCount;
        searchInfo->seekDocCount = seekTermDocCount;
        result->setPhaseOneSearchInfo(searchInfo);
        const suez::ServiceInfo &serviceInfo = sessionResource->serviceInfo;
        string zoneBizName = serviceInfo.getZoneName()  + "." + sessionResource->bizName;
        autil::PartitionRange utilRange;
        if (!RangeUtil::getRange(serviceInfo.getPartCount(), serviceInfo.getPartId(), utilRange)) {
            AUTIL_LOG(ERROR, "get Range failed");
        }
        result->addCoveredRange(zoneBizName, utilRange.first, utilRange.second);

        auto userMetricReporter = queryResource->getQueryMetricsReporter();
        if (likely(userMetricReporter != NULL)) {
            userMetricReporter->report(aggCount, "aggCount", kmonitor::GAUGE, &_tags);
            userMetricReporter->report(matchCount, "matchCount", kmonitor::GAUGE, &_tags);
        }

        Ha3ResultVariant ha3Result(result, queryResource->getPool());
        Tensor* out = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &out));
        out->scalar<Variant>()() = ha3Result;
    }

private:
    kmonitor::MetricsTags _tags;
private:
    AUTIL_LOG_DECLARE();

};

AUTIL_LOG_SETUP(ha3, AggResultConstructOp);

REGISTER_KERNEL_BUILDER(Name("AggResultConstructOp")
                        .Device(DEVICE_CPU),
                        AggResultConstructOp);

} // namespace turing
} // namespace isearch
