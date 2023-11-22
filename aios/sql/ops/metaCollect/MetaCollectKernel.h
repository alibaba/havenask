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

#include <string>
#include <vector>

#include "navi/common.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/ScopeTerminatorKernel.h"
#include "sql/proto/SqlSearchInfoCollectorR.h"

namespace navi {
class KernelComputeContext;
} // namespace navi

namespace sql {

class MetaCollectKernel : public navi::ScopeTerminatorKernel {
public:
    MetaCollectKernel();
    ~MetaCollectKernel();
    MetaCollectKernel(const MetaCollectKernel &) = delete;
    MetaCollectKernel &operator=(const MetaCollectKernel &) = delete;

public:
    std::string getName() const override;
    std::vector<std::string> getOutputs() const override;
    navi::ErrorCode compute(navi::KernelComputeContext &ctx) override;

public:
    static const std::string KERNEL_ID;
    static const std::string OUTPUT_PORT;

private:
    void handleScopeError(navi::KernelComputeContext &ctx) const;

private:
    KERNEL_DEPEND_DECLARE();

private:
    KERNEL_DEPEND_ON(SqlSearchInfoCollectorR, _sqlSearchInfoCollectorR);
};

} // namespace sql
