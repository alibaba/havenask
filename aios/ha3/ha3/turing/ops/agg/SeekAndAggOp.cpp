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
#include <stdint.h>
#include <cstddef>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/ClauseBase.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/Request.h"
#include "ha3/common/Tracer.h"
#include "ha3/search/Aggregator.h"
#include "ha3/turing/ops/ha3_op_common.h"
#include "ha3/turing/variant/AggregatorVariant.h"
#include "ha3/turing/variant/ExpressionResourceVariant.h"
#include "ha3/turing/variant/SeekIteratorVariant.h"
#include "indexlib/util/metrics/Monitor.h"
#include "kmonitor/client/MetricType.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/ValueType.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/op_kernel.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_shape.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"
#include "tensorflow/core/platform/byte_order.h"
#include "turing_ops_util/util/OpUtil.h"

using namespace std;
using namespace tensorflow;
using namespace matchdoc;
using namespace suez::turing;

using namespace isearch::search;
using namespace isearch::common;

namespace isearch {
namespace turing {

class SeekAndAggOp : public tensorflow::OpKernel
{
public:
    explicit SeekAndAggOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
        OP_REQUIRES_OK(ctx, ctx->GetAttr("batch_size", &_batchSize));
        _seekCountName = "user.op.seekCount";
        ADD_OP_BASE_TAG(_tags);
    }
public:
    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto sessionResource = GET_SESSION_RESOURCE();
        auto queryResource = GET_QUERY_RESOURCE(sessionResource);

        auto aggVariant = ctx->input(0).scalar<Variant>()().get<AggregatorVariant>();
        OP_REQUIRES(ctx, aggVariant, errors::Unavailable("get agg variant failed"));

        auto seekIteratorVariant = ctx->input(1).scalar<Variant>()().get<SeekIteratorVariant>();
        OP_REQUIRES(ctx, seekIteratorVariant, errors::Unavailable("get seek iterator variant failed"));
        SeekIteratorPtr seekIterator = seekIteratorVariant->getSeekIterator();
        if (!seekIterator) {
            ctx->set_output(0, ctx->input(0));
            return;
        }
        ExpressionResourcePtr resource = aggVariant->getExpressionResource();
        OP_REQUIRES(ctx, resource, errors::Unavailable("get expression resource failed"));
        RequestPtr request = resource->_request;
        AggregatorPtr aggregator = aggVariant->getAggregator();
        OP_REQUIRES(ctx, aggregator, errors::Unavailable("get aggregator failed"));
        int32_t rankSize = getRankSize(request);
        uint32_t seekCount = doSeekAndAgg(seekIterator, aggregator, rankSize);
        auto userMetricReporter = queryResource->getQueryMetricsReporter();
        if (likely(userMetricReporter != NULL)) {
            userMetricReporter->report(seekCount, _seekCountName, kmonitor::GAUGE, &_tags);
        }
        AggregatorVariant newAggVariant = *aggVariant;
        newAggVariant.setSeekedCount(seekIterator->getSeekTimes());
        newAggVariant.setSeekTermDocCount(seekIterator->getSeekDocCount());

        Tensor *outputTensor = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, TensorShape({}), &outputTensor));
        outputTensor->scalar<Variant>()() = newAggVariant;
    }

private:
    int32_t getRankSize(const RequestPtr &request) {
        int32_t rankSize = 0;
        auto configClause = request->getConfigClause();
        if (configClause) {
            rankSize = configClause->getRankSize();
        }
        if (rankSize == 0) {
            rankSize = numeric_limits<int32_t>::max();
        }
        return rankSize;
    }
    uint32_t doSeekAndAgg(const SeekIteratorPtr &seekIterator,
                      const AggregatorPtr &aggregator,
                      int32_t rankSize)
    {
        uint32_t seekCount = rankSize;
        MatchDocAllocatorPtr allocator = seekIterator->getMatchDocAllocator();
        bool eof = false;
        if (_batchSize > 1) {
            int32_t size = 0;
            vector<matchdoc::MatchDoc> matchDocs;
            matchDocs.reserve(_batchSize);
            while (rankSize > 0 && !eof) {
                size = rankSize < _batchSize ? rankSize : _batchSize;
                eof = seekIterator->batchSeek(size, matchDocs);
                aggregator->batchAggregate(matchDocs);
                rankSize -= matchDocs.size();
                for (auto matchDoc : matchDocs) {
                    allocator->deallocate(matchDoc);
                }
                matchDocs.clear();
            }
        } else {
            while (rankSize > 0) {
                matchdoc::MatchDoc doc = seekIterator->seek();
                if (matchdoc::INVALID_MATCHDOC == doc) {
                    break;
                }
                aggregator->aggregate(doc);
                allocator->deallocate(doc);
                rankSize--;
            }
        }
        return seekCount - rankSize;
    }

private:
    AUTIL_LOG_DECLARE();
private:
private:
    kmonitor::MetricsTags _tags;
    string _seekCountName;
    int _batchSize;
};

AUTIL_LOG_SETUP(ha3, SeekAndAggOp);

REGISTER_KERNEL_BUILDER(Name("SeekAndAggOp")
                        .Device(DEVICE_CPU),
                        SeekAndAggOp);

} // namespace turing
} // namespace isearch
