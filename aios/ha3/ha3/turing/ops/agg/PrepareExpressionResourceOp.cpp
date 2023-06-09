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
#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "alog/Logger.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/ClauseBase.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/Request.h"
#include "ha3/common/Tracer.h"
#include "ha3/common/VirtualAttributeClause.h"
#include "ha3/isearch.h"
#include "ha3/turing/ops/agg/PrepareExpressionResourceOp.h"
#include "ha3/turing/variant/ExpressionResourceVariant.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "indexlib/misc/common.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/ValueType.h"
#include "suez/turing/common/BizInfo.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "suez/turing/expression/cava/common/CavaPluginManager.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"
#include "suez/turing/expression/function/FunctionInterfaceCreator.h"
#include "suez/turing/expression/provider/FunctionProvider.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/op_kernel.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_shape.h"
#include "tensorflow/core/framework/tensor_shape.pb.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"
#include "turing_ops_util/util/OpUtil.h"

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
namespace suez {
namespace turing {
class SuezCavaAllocator;
class VirtualAttribute;
}  // namespace turing
}  // namespace suez

using namespace tensorflow;
using namespace matchdoc;
using namespace suez::turing;
using namespace isearch::common;

namespace isearch {
namespace turing {
AUTIL_LOG_SETUP(ha3, PrepareExpressionResourceOp);


void PrepareExpressionResourceOp::Compute(tensorflow::OpKernelContext* ctx) {
    auto sessionResource = GET_SESSION_RESOURCE();
    auto queryResource = GET_QUERY_RESOURCE(sessionResource);
    auto requestVariant = ctx->input(0).scalar<Variant>()().get<Ha3RequestVariant>();
    OP_REQUIRES(ctx, requestVariant, errors::Unavailable("get ha3 request variant failed"));
    RequestPtr request = requestVariant->getRequest();
    OP_REQUIRES(ctx, request, errors::Unavailable("get ha3 request failed"));
    auto tableInfoPtr = sessionResource->tableInfo;
    OP_REQUIRES(ctx, tableInfoPtr, errors::Unavailable("get table info failed"));

    ExpressionResourcePtr resource = createExpressionResource(request, tableInfoPtr,
            sessionResource->bizInfo->_itemTableName, sessionResource->functionInterfaceCreator,
            queryResource->getIndexSnapshot(), queryResource->getTracer(),
            sessionResource->cavaPluginManager, queryResource->getPool(),
            queryResource->getCavaAllocator());
    OP_REQUIRES(ctx, resource, errors::Unavailable("create expression resource failed"));

    ExpressionResourceVariant resourceVariant(resource);
    Tensor *outputTensor = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(0, TensorShape({}), &outputTensor));
    outputTensor->scalar<Variant>()() = resourceVariant;
}

ExpressionResourcePtr PrepareExpressionResourceOp::createExpressionResource(
        const RequestPtr &request,
        const TableInfoPtr &tableInfoPtr,
        const string &mainTableName,
        const FunctionInterfaceCreatorPtr &functionInterfaceCreator,
        indexlib::partition::PartitionReaderSnapshot *partitionReaderSnapshot,
        Tracer *tracer,
        const suez::turing::CavaPluginManagerPtr &cavaPluginManager,
        autil::mem_pool::Pool *pool,
        suez::turing::SuezCavaAllocator *cavaAllocator)
{
    common::ConfigClause *configClause = request->getConfigClause();
    if (configClause == NULL) {
        AUTIL_LOG(WARN, "config clause is empty.");
        return ExpressionResourcePtr();
    }
    bool useSub = useSubDoc(request.get(), tableInfoPtr.get());
    MatchDocAllocatorPtr matchDocAllocator(new MatchDocAllocator(pool, useSub));
    FunctionProviderPtr functionProvider;
    functionProvider.reset(new FunctionProvider(matchDocAllocator.get(),
                    pool, cavaAllocator, tracer, partitionReaderSnapshot,
                    configClause->getKeyValueMapPtr().get()));

    std::vector<suez::turing::VirtualAttribute*> virtualAttributes;
    auto virtualAttributeClause = request->getVirtualAttributeClause();
    if (virtualAttributeClause) {
        virtualAttributes = virtualAttributeClause->getVirtualAttributes();
    }
    AttributeExpressionCreatorPtr attributeExpressionCreator;
    attributeExpressionCreator.reset(new AttributeExpressionCreator(
                    pool, matchDocAllocator.get(),
                    mainTableName, partitionReaderSnapshot,
                    tableInfoPtr, virtualAttributes,
                    functionInterfaceCreator.get(),
                    cavaPluginManager.get(), functionProvider.get()));

    ExpressionResourcePtr resource(new ExpressionResource(request, matchDocAllocator,
                    functionProvider, attributeExpressionCreator));
    return resource;
}

bool PrepareExpressionResourceOp::useSubDoc(const Request *request, const TableInfo *tableInfo) {
    if (!request) {
        return false;
    }
    common::ConfigClause *configClause = request->getConfigClause();
    if (configClause != NULL
        && configClause->getSubDocDisplayType() != SUB_DOC_DISPLAY_NO
        && tableInfo && tableInfo->hasSubSchema())
    {
        return true;
    }
    return false;
}

REGISTER_KERNEL_BUILDER(Name("PrepareExpressionResourceOp")
                        .Device(DEVICE_CPU),
                        PrepareExpressionResourceOp);

} // namespace turing
} // namespace isearch
