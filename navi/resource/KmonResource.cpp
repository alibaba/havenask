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
#include "navi/resource/KmonResource.h"
#include "navi/builder/ResourceDefBuilder.h"

using namespace kmonitor;

namespace navi {

KmonResourceBase::KmonResourceBase(MetricsReporterPtr baseReporter)
    : _baseReporter(baseReporter)
{
}

KmonResourceBase::~KmonResourceBase() {
    NAVI_KERNEL_LOG(INFO, "kmon resource destructed [%p]", this);
}

bool KmonResourceBase::config(ResourceConfigContext &ctx) {
    return false;
}

ErrorCode KmonResourceBase::init(ResourceInitContext &ctx) {
    return EC_ABORT;
}

RootKmonResource::RootKmonResource(MetricsReporterPtr baseReporter)
    : KmonResourceBase(baseReporter)
{
}

void RootKmonResource::def(ResourceDefBuilder &builder) const {
    builder
        .name(ROOT_KMON_RESOURCE_ID);
}

// DO NOT REGISTER RootKmonResource
// this resource is created by framework

BizKmonResource::BizKmonResource()
    : KmonResourceBase(nullptr)
{
}

void BizKmonResource::def(ResourceDefBuilder &builder) const {
    builder.name(BIZ_KMON_RESOURCE_ID)
        .depend(ROOT_KMON_RESOURCE_ID, true, BIND_RESOURCE_TO(_rootKmonResource))
        // ATTENTION:
        // make sure biz kmon resource destructed before turing service snapshot (for plugin so),
        // TODO: delete this when suez_navi completed
        .depend("SqlBizResource", false, BIND_RESOURCE_TO(_unusedSqlBizResource));
}

bool BizKmonResource::config(ResourceConfigContext &ctx) {
    return true;
}

ErrorCode BizKmonResource::init(ResourceInitContext &ctx) {
    auto rootReporter = _rootKmonResource->getBaseReporter();
    assert(rootReporter != nullptr);
    MetricsTags tags;
    tags.AddTag("biz", ctx.getBizName());
    tags.AddTag("sub_part_id", std::to_string(ctx.getPartId()));
    // ATTENTION:
    // do not use `getSubReporter` to avoid caching _metricsReporter by parent reporter,
    // otherwise _metricsReporter may call unloaded symbols from plugin so while deallocation
    _baseReporter = rootReporter->newSubReporter("", tags);
    NAVI_KERNEL_LOG(INFO,
                    "biz [%s] partCount [%d] partId [%d] create biz kmon resource [%p]",
                    ctx.getBizName().c_str(), ctx.getPartCount(), ctx.getPartId(), this);
    return EC_NONE;
}

REGISTER_RESOURCE(BizKmonResource);

}
