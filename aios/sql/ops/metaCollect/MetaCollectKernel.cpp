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
#include "sql/ops/metaCollect/MetaCollectKernel.h"

#include <memory>

#include "navi/engine/KernelComputeContext.h"
#include "sql/ops/metaCollect/SqlMetaData.h"
#include "sql/proto/SqlSearchInfoCollector.h"

namespace sql {

const std::string MetaCollectKernel::KERNEL_ID = "sql.meta_collector";
const std::string MetaCollectKernel::OUTPUT_PORT = "out";

MetaCollectKernel::MetaCollectKernel() {}

MetaCollectKernel::~MetaCollectKernel() {}

std::string MetaCollectKernel::getName() const {
    return KERNEL_ID;
}

std::vector<std::string> MetaCollectKernel::getOutputs() const {
    return {OUTPUT_PORT};
}

navi::ErrorCode MetaCollectKernel::compute(navi::KernelComputeContext &ctx) {
    const auto &collector = _sqlSearchInfoCollectorR->getCollector();
    auto newCollector = std::make_shared<SqlSearchInfoCollector>();
    collector->stealTo(*newCollector);
    newCollector->shrinkOverwrite();
    auto metaData = std::make_shared<SqlMetaData>(newCollector);
    handleScopeError(ctx);
    ctx.setOutput(0, metaData, true);
    return navi::EC_NONE;
}

void MetaCollectKernel::handleScopeError(navi::KernelComputeContext &ctx) const {
    auto error = ctx.getScopeError();
    auto strategy = ctx.getScopeErrorHandleStrategy();
    if (error && navi::EHS_ERROR_AS_FATAL == strategy) {
        ctx.reportScopeError(error);
    }
}

REGISTER_KERNEL(MetaCollectKernel);

} // namespace sql
