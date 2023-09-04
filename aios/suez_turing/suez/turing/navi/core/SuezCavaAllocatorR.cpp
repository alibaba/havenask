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
#include "suez/turing/navi/SuezCavaAllocatorR.h"

namespace suez {
namespace turing {

const std::string SuezCavaAllocatorR::RESOURCE_ID = "suez_turing.cava_allocator_r";

SuezCavaAllocatorR::SuezCavaAllocatorR() {}

SuezCavaAllocatorR::~SuezCavaAllocatorR() {}

void SuezCavaAllocatorR::def(navi::ResourceDefBuilder &builder) const { builder.name(RESOURCE_ID, navi::RS_SUB_GRAPH); }

bool SuezCavaAllocatorR::config(navi::ResourceConfigContext &ctx) { return true; }

navi::ErrorCode SuezCavaAllocatorR::init(navi::ResourceInitContext &ctx) {
    const auto &cavaConfig = _cavaJitWrapperR->getCavaConfig();
    auto sizeLimit = cavaConfig._allocSizeLimit * 1024 * 1024;
    _allocator.reset(new SuezCavaAllocator(_queryMemPoolR->getPool().get(), sizeLimit));
    return navi::EC_NONE;
}

const SuezCavaAllocatorPtr &SuezCavaAllocatorR::getAllocator() const { return _allocator; }

REGISTER_RESOURCE(SuezCavaAllocatorR);

} // namespace turing
} // namespace suez
