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
#include "navi/ops/PlaceholderKernel.h"

namespace navi {

PlaceholderKernel::PlaceholderKernel() {
}

PlaceholderKernel::~PlaceholderKernel() {
}

void PlaceholderKernel::def(KernelDefBuilder &builder) const {
    builder
        .name(PLACEHOLDER_KERNEL_NAME)
        .output(PLACEHOLDER_OUTPUT_PORT, "");
}

ErrorCode PlaceholderKernel::init(KernelInitContext &ctx) {
    return EC_NONE;
}

ErrorCode PlaceholderKernel::compute(KernelComputeContext &ctx) {
    NAVI_KERNEL_LOG(ERROR, "placeholder must be fed");
    return EC_ABORT;
}

REGISTER_KERNEL(PlaceholderKernel);

}

