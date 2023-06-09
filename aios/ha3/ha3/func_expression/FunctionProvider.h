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

#include "suez/turing/expression/provider/FunctionProvider.h"

#include "suez/turing/expression/provider/ProviderBase.h"

#include "ha3/common/Tracer.h"
#include "ha3/search/ProviderBase.h"
#include "ha3/search/SearchPluginResource.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace func_expression {

typedef search::SearchPluginResource FunctionResource;

class FunctionProvider : public suez::turing::FunctionProvider,
                         public search::ProviderBase
{
public:
    FunctionProvider(const FunctionResource &resource);
    ~FunctionProvider();
    void setQueryTerminator(common::TimeoutTerminator *queryTerminator) {
        _resource.queryTerminator = queryTerminator;
    }
    void setRequestTracer(common::Tracer *tracer) {
        _resource.requestTracer = tracer;
        suez::turing::ProviderBase::setRequestTracer(tracer);
    }
private:
    FunctionProvider(const FunctionProvider &);
    FunctionProvider& operator=(const FunctionProvider &);
private:
    search::SearchPluginResource _resource;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FunctionProvider> FunctionProviderPtr;

} // namespace func_expression
} // namespace isearch

