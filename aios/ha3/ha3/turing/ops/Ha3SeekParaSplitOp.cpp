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
#include "ha3/turing/ops/Ha3SeekParaSplitOp.h"

#include <iosfwd>
#include <memory>
#include <memory>
#include <vector>

#include "autil/mem_pool/MemoryChunk.h"
#include "turing_ops_util/util/OpUtil.h"
#include "turing_ops_util/variant/MultiValueVariant.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_shape.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"
#include "tensorflow/core/platform/byte_order.h"

#include "ha3/common/ClauseBase.h"
#include "ha3/common/Request.h"
#include "ha3/common/Tracer.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/LayerMetaUtil.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/turing/ops/SearcherQueryResource.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "ha3/turing/variant/LayerMetasVariant.h"

using namespace autil::mem_pool;
using namespace tensorflow;
using namespace suez::turing;
using namespace std;

using namespace isearch::common;
using namespace isearch::search;

namespace isearch {
namespace turing {
AUTIL_LOG_SETUP(ha3, Ha3SeekParaSplitOp);

void Ha3SeekParaSplitOp::Compute(tensorflow::OpKernelContext* ctx) {
    auto sessionResource = GET_SESSION_RESOURCE();
    auto queryResource = GET_QUERY_RESOURCE(sessionResource);
    auto requestVariant =
        ctx->input(0).scalar<Variant>()().get<Ha3RequestVariant>();
    OP_REQUIRES(ctx, requestVariant,
                errors::Unavailable("get ha3 request variant failed"));
    RequestPtr request = requestVariant->getRequest();
    OP_REQUIRES(ctx, request, errors::Unavailable("get ha3 request failed"));
    SearcherQueryResource *searcherQueryResource =
        dynamic_cast<SearcherQueryResource *>(queryResource);
    OP_REQUIRES(ctx, searcherQueryResource,
                errors::Unavailable("get searcher query resource failed"));
    const LayerMetasPtr &layerMetas = LayerMetaUtil::createLayerMetas(
            request->getQueryLayerClause(),
            request->getRankClause(),
            searcherQueryResource->indexPartitionReaderWrapper.get(),
            searcherQueryResource->getPool());
    OP_REQUIRES(ctx, layerMetas, errors::Unavailable("create layer metas failed"));

    std::vector<LayerMetasPtr> splitedLayerMetas;
    auto readerWrapper = searcherQueryResource->indexPartitionReaderWrapper;
    bool ret = LayerMetaUtil::splitLayerMetas(searcherQueryResource->getPool(),
            *layerMetas, readerWrapper, _outputNum, splitedLayerMetas);
    OP_REQUIRES(ctx, ret, errors::Unavailable("split layer metas failed"));

    for (int idx = 0; idx < _outputNum; idx++) {
        LayerMetasVariant outVariant(splitedLayerMetas[idx]);
        Tensor *layerMetaTensor = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(
                        idx, TensorShape({}), &layerMetaTensor));
        layerMetaTensor->scalar<Variant>()() = outVariant;
    }

    auto metricsCollector = searcherQueryResource->sessionMetricsCollector;
    if (metricsCollector) {
        metricsCollector->parallelSeekStartTrigger();
    }
}

REGISTER_KERNEL_BUILDER(Name("Ha3SeekParaSplitOp")
                        .Device(DEVICE_CPU),
                        Ha3SeekParaSplitOp);

} // namespace turing
} // namespace isearch
