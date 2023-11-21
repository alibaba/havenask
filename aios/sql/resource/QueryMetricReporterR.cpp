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
#include "sql/resource/QueryMetricReporterR.h"

#include <engine/NaviConfigContext.h>
#include <memory>

#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/Resource.h"
#include "navi/log/NaviLogger.h"
#include "navi/proto/KernelDef.pb.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

namespace sql {

const std::string QueryMetricReporterR::RESOURCE_ID = "query_metric_reporter_r";

QueryMetricReporterR::QueryMetricReporterR()
    : _metricsReporterR(nullptr) {}

QueryMetricReporterR::~QueryMetricReporterR() {}

void QueryMetricReporterR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_SUB_GRAPH);
}

bool QueryMetricReporterR::config(navi::ResourceConfigContext &ctx) {
    NAVI_JSONIZE(ctx, "tags", _tagsMap, _tagsMap);
    NAVI_JSONIZE(ctx, "path", _path, _path);
    return true;
}

navi::ErrorCode QueryMetricReporterR::init(navi::ResourceInitContext &ctx) {
    _tagsMap["biz"] = ctx.getBizName();
    _tagsMap["sub_part_id"] = std::to_string(ctx.getPartId());
    const auto &reporter = _metricsReporterR->getQueryMetricsReporter(_tagsMap, _path);
    if (!reporter) {
        NAVI_KERNEL_LOG(ERROR, "init QueryMetricReporterR failed, path: [%s]", _path.c_str());
        return navi::EC_ABORT;
    }
    _reporter = reporter;
    return navi::EC_NONE;
}

const kmonitor::MetricsReporterPtr &QueryMetricReporterR::getReporter() const {
    return _reporter;
}

REGISTER_RESOURCE(QueryMetricReporterR);

} // namespace sql
