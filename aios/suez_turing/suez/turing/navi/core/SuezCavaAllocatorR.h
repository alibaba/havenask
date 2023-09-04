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

#include "navi/engine/Resource.h"
#include "suez/turing/expression/cava/common/CavaJitWrapperR.h"
#include "suez/turing/expression/cava/common/SuezCavaAllocator.h"
#include "suez/turing/navi/QueryMemPoolR.h"

namespace suez {
namespace turing {

class SuezCavaAllocatorR : public navi::Resource {
public:
    SuezCavaAllocatorR();
    ~SuezCavaAllocatorR();
    SuezCavaAllocatorR(const SuezCavaAllocatorR &) = delete;
    SuezCavaAllocatorR &operator=(const SuezCavaAllocatorR &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    const SuezCavaAllocatorPtr &getAllocator() const;

public:
    static const std::string RESOURCE_ID;

private:
    RESOURCE_DEPEND_DECLARE();

private:
    RESOURCE_DEPEND_ON(QueryMemPoolR, _queryMemPoolR);
    RESOURCE_DEPEND_ON(CavaJitWrapperR, _cavaJitWrapperR);
    SuezCavaAllocatorPtr _allocator;
};

NAVI_TYPEDEF_PTR(SuezCavaAllocatorR);

} // namespace turing
} // namespace suez
