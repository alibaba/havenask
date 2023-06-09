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
#include "ha3/turing/ops/agg/LayerMetasCreateOp.h"

#include <cstddef>
#include <memory>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/mem_pool/PoolBase.h"
#include "autil/mem_pool/PoolVector.h"
#include "ha3/common/ClauseBase.h"
#include "ha3/common/Request.h"
#include "ha3/search/IndexPartitionReaderUtil.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/LayerMetasCreator.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "ha3/turing/variant/LayerMetasVariant.h"
#include "suez/turing/common/BizInfo.h"
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
#include "turing_ops_util/util/OpUtil.h"
#include "turing_ops_util/variant/MultiValueVariant.h"
#include "turing_ops_util/variant/Tracer.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace indexlib {
namespace partition {
class PartitionReaderSnapshot;
}  // namespace partition
}  // namespace indexlib
namespace isearch {
namespace common {
class QueryLayerClause;
}  // namespace common
}  // namespace isearch

using namespace tensorflow;
using namespace suez::turing;
using namespace std;

using namespace isearch::common;
using namespace isearch::search;

namespace isearch {
namespace turing {
AUTIL_LOG_SETUP(ha3, LayerMetasCreateOp);

void LayerMetasCreateOp::Compute(tensorflow::OpKernelContext* ctx) {
    // get search query resource
    auto sessionResource = GET_SESSION_RESOURCE();
    auto queryResource = GET_QUERY_RESOURCE(sessionResource);

    auto requestVariant = ctx->input(0).scalar<Variant>()().get<Ha3RequestVariant>();
    OP_REQUIRES(ctx, requestVariant, errors::Unavailable("get ha3 request variant failed"));
    RequestPtr request = requestVariant->getRequest();
    OP_REQUIRES(ctx, request, errors::Unavailable("get ha3 request failed"));

    string mainTableName = sessionResource->bizInfo->_itemTableName;
    LayerMetasPtr layerMetas = createLayerMetas(queryResource->getIndexSnapshot(),
            mainTableName, queryResource->getPool(), request->getQueryLayerClause());
    OP_REQUIRES(ctx, layerMetas, errors::Unavailable("create layer metas failed"));
    updateQuota(layerMetas);
    LayerMetasVariant layerMetasVariant(layerMetas);
    Tensor *layerMetaTensor = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(0, TensorShape({}), &layerMetaTensor));
    layerMetaTensor->scalar<Variant>()() = layerMetasVariant;
}

LayerMetasPtr LayerMetasCreateOp::createLayerMetas(
        indexlib::partition::PartitionReaderSnapshot *indexSnapshot,
        const std::string &mainTableName,
        autil::mem_pool::Pool *pool,
        QueryLayerClause* queryLayerClause)
{
    IndexPartitionReaderWrapperPtr readerWrapper =
        IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(indexSnapshot,
                mainTableName);
    if (readerWrapper == nullptr) {
        AUTIL_LOG(WARN, "create index partition reader wrapper failed.");
        return LayerMetasPtr();
    }
    readerWrapper->setSessionPool(pool);
    LayerMetasCreator layerMetasCreator;
    layerMetasCreator.init(pool, readerWrapper.get());
    LayerMetasPtr layerMetas(layerMetasCreator.create(queryLayerClause),
                                [](LayerMetas* p) {
                POOL_DELETE_CLASS(p);
            });
    return layerMetas;
}

void LayerMetasCreateOp::updateQuota(LayerMetasPtr &layerMetas) {
    if (layerMetas == NULL) {
        return;
    }
    for (auto &layerMeta : *layerMetas) {
        for (auto &rangeMeta : layerMeta) {
            rangeMeta.quota =  rangeMeta.end - rangeMeta.begin + 1;
        }
    }
}

REGISTER_KERNEL_BUILDER(Name("LayerMetasCreateOp")
                        .Device(DEVICE_CPU),
                        LayerMetasCreateOp);

} // namespace turing
} // namespace isearch
