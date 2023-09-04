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
#include "sql/resource/TraceAdapterR.h"

#include <engine/NaviConfigContext.h>
#include <memory>

#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/Resource.h"
#include "navi/proto/KernelDef.pb.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

namespace sql {

const std::string TraceAdapterR::RESOURCE_ID = "trace_adapter_r";

TraceAdapterR::TraceAdapterR() {}

TraceAdapterR::~TraceAdapterR() {}

void TraceAdapterR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_SUB_GRAPH);
}

bool TraceAdapterR::config(navi::ResourceConfigContext &ctx) {
    NAVI_JSONIZE(ctx, "level", _traceLevel, _traceLevel);
    return true;
}

navi::ErrorCode TraceAdapterR::init(navi::ResourceInitContext &ctx) {
    if (!_traceLevel.empty()) {
        _tracer.reset(new TracerAdapter());
        _tracer->setLevel(_traceLevel);
    }
    return navi::EC_NONE;
}

const TracerAdapterPtr &TraceAdapterR::getTracer() const {
    return _tracer;
}

REGISTER_RESOURCE(TraceAdapterR);

} // namespace sql
