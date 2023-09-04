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

#include "navi/engine/Kernel.h"
#include "sql/ops/join/LookupJoinKernel.h"

namespace navi {
class KernelDefBuilder;
} // namespace navi

namespace sql {

class ExternalLookupJoinKernel : public LookupJoinKernel {
public:
    ExternalLookupJoinKernel();
    ~ExternalLookupJoinKernel();
    ExternalLookupJoinKernel(const ExternalLookupJoinKernel &) = delete;
    ExternalLookupJoinKernel &operator=(const ExternalLookupJoinKernel &) = delete;

public:
    void def(navi::KernelDefBuilder &builder) const override;

private:
    KERNEL_DEPEND_DECLARE_BASE(LookupJoinKernel);
};

} // namespace sql
