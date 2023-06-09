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
#include "ha3/turing/ops/agg/AggPrepareOp.h"

#include <memory>
#include <string>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/jsonizable.h"
#include "ha3/common/AggregateResult.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/Request.h"
#include "ha3/common/Tracer.h"
#include "ha3/config/AggSamplerConfigInfo.h"
#include "ha3/search/Aggregator.h"
#include "ha3/search/AggregatorCreator.h"
#include "ha3/turing/variant/AggregatorVariant.h"
#include "ha3/turing/variant/ExpressionResourceVariant.h"
#include "matchdoc/MatchDocAllocator.h"
#include "resource_reader/ResourceReader.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"
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

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

using namespace tensorflow;
using namespace suez::turing;

using namespace isearch::search;
using namespace isearch::common;

namespace isearch {
namespace turing {
AUTIL_LOG_SETUP(ha3, AggPrepareOp);

AggPrepareOp::AggPrepareOp(tensorflow::OpKernelConstruction* ctx)
    : tensorflow::OpKernel(ctx)
{     
    auto sessionResource = GET_SESSION_RESOURCE();
    auto resourceReader = sessionResource->resourceReader;
    OP_REQUIRES(ctx, resourceReader, errors::Internal("resource reader is null."));
    AUTIL_LOG(INFO, "get config root path[%s]", resourceReader->getConfigPath().c_str());

    std::string relativePath, jsonPath;
    OP_REQUIRES_OK(ctx, ctx->GetAttr("relative_path", &relativePath));
    OP_REQUIRES_OK(ctx, ctx->GetAttr("json_path", &jsonPath));
    if (!relativePath.empty() && !jsonPath.empty()) {
        bool ret = resourceReader->getConfigWithJsonPath(relativePath, jsonPath, _aggSamplerConfig);
        if (!ret) {
            AUTIL_LOG(WARN, "get agg sample config  fail, file [%s], path [%s]", relativePath.c_str(),
                    jsonPath.c_str());
        } else {
            if (_aggSamplerConfig.getAggBatchMode()) {
                _aggSamplerConfig.setAggBatchMode(false);
            }
        }
    }
}

void AggPrepareOp::Compute(tensorflow::OpKernelContext* ctx) {
    auto sessionResource = GET_SESSION_RESOURCE();
    auto queryResource = GET_QUERY_RESOURCE(sessionResource);
    
    auto resourceVariant = ctx->input(0).scalar<Variant>()().get<ExpressionResourceVariant>();
    OP_REQUIRES(ctx, resourceVariant, errors::Unavailable("get expression resource variant failed"));
    ExpressionResourcePtr resource = resourceVariant->getExpressionResource();
    OP_REQUIRES(ctx, resource, errors::Unavailable("expression resource is empty."));

    AggregatorPtr aggregator = createAggregator(resource, queryResource, _aggSamplerConfig);
    OP_REQUIRES(ctx, aggregator, errors::Unavailable("prepare aggregator failed."));

    AggregatorVariant aggregatorVariant(aggregator, resource);
    Tensor *outputTensor = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(0, TensorShape({}), &outputTensor));
    outputTensor->scalar<Variant>()() = aggregatorVariant;
}

AggregatorPtr AggPrepareOp::createAggregator(const ExpressionResourcePtr &resource,
        suez::turing::QueryResource *queryResource,
        const config::AggSamplerConfigInfo &configInfo)
{
    config::AggSamplerConfigInfo aggSamplerConfigInfo = configInfo;
    autil::mem_pool::Pool *pool = queryResource->getPool();
    auto configClause = resource->_request->getConfigClause();
    if (configClause) {
        const auto &enableAggJit = configClause->getKVPairValue("enableAggJit");
        if (!enableAggJit.empty()) {
            bool enableOrNot = enableAggJit == "true" ? true : false;
            aggSamplerConfigInfo.setEnableJit(enableOrNot);
        }
    }
    AggregatorCreator aggCreator(resource->_attributeExpressionCreator.get(), pool, queryResource);
    aggCreator.setAggSamplerConfigInfo(aggSamplerConfigInfo);
    AggregatorPtr aggregator(aggCreator.createAggregator(
                    resource->_request->getAggregateClause()));
    if (aggregator == nullptr) {
        AUTIL_LOG(WARN, "create aggregator failed.");        
        return aggregator;
    }
    matchdoc::MatchDocAllocatorPtr matchDocAllocator = resource->_matchDocAllocator;
    aggregator->setMatchDocAllocator(matchDocAllocator.get());
    return aggregator;
}

REGISTER_KERNEL_BUILDER(Name("AggPrepareOp")
                        .Device(DEVICE_CPU),
                        AggPrepareOp);

} // namespace turing
} // namespace isearch
