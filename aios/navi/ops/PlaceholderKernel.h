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
#ifndef NAVI_PLACEHOLDERKERNEL_H
#define NAVI_PLACEHOLDERKERNEL_H

#include "navi/engine/Kernel.h"

namespace navi {

class PlaceholderKernel : public Kernel
{
public:
    PlaceholderKernel();
    ~PlaceholderKernel();
    PlaceholderKernel(const PlaceholderKernel &) = delete;
    PlaceholderKernel &operator=(const PlaceholderKernel &) = delete;
public:
    void def(KernelDefBuilder &builder) const override;
    ErrorCode init(KernelInitContext &ctx) override;
    ErrorCode compute(KernelComputeContext &ctx) override;
};

}

#endif //NAVI_PLACEHOLDERKERNEL_H
