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
#include "ha3/func_expression/FunctionProvider.h"

#include <memory>

#include "suez/turing/expression/provider/FunctionProvider.h"

#include "ha3/common/Ha3MatchDocAllocator.h"
#include "autil/Log.h"

namespace isearch {
namespace func_expression {
AUTIL_LOG_SETUP(ha3, FunctionProvider);

FunctionProvider::FunctionProvider(const FunctionResource &resource)
    : suez::turing::FunctionProvider(resource.matchDocAllocator.get(),
            resource.pool,
            resource.cavaAllocator,
            resource.requestTracer,
            resource.partitionReaderSnapshot,
            resource.kvpairs)
    , _resource(resource)
{
    setSearchPluginResource(&_resource);
    setupTraceRefer(convertRankTraceLevel(getRequest()));
}

FunctionProvider::~FunctionProvider() {
}

} // namespace func_expression
} // namespace isearch
