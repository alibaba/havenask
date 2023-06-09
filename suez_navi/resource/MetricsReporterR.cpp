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
#include "suez_navi/resource/MetricsReporterR.h"
#include "autil/StringUtil.h"

namespace suez_navi {

static const std::string KMONITOR_KEYVALUE_SEP = "^";
static const std::string KMONITOR_MULTI_SEP = "@";

const std::string MetricsReporterR::RESOURCE_ID = "metrics_reporter_r";

MetricsReporterR::MetricsReporterR() {
}

MetricsReporterR::~MetricsReporterR() {
}

void MetricsReporterR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID);
}

bool MetricsReporterR::config(navi::ResourceConfigContext &ctx) {
    ctx.Jsonize("metrics_prefix", _prefix, _prefix);
    ctx.Jsonize("metrics_path", _path, _path);
    std::string tagStr;
    ctx.Jsonize("tags", tagStr, tagStr);

    std::map<std::string, std::string> kmonitorTags;
    if (!parseKMonitorTags(tagStr, kmonitorTags)) {
        return false;
    }
    _tags = kmonitor::MetricsTags(kmonitorTags);
    return true;
}

bool MetricsReporterR::parseKMonitorTags(
    const std::string &tagsStr, std::map<std::string, std::string> &tagsMap)
{
    auto tagVec = autil::StringUtil::split(tagsStr, KMONITOR_MULTI_SEP);
    for (const auto &tags : tagVec) {
        auto kvVec = autil::StringUtil::split(tags, KMONITOR_KEYVALUE_SEP);
        if (kvVec.size() != 2) {
            NAVI_KERNEL_LOG(WARN, "parse kmonitor tags [%s] failed.",
                            tags.c_str());
            return false;
        }
        autil::StringUtil::trim(kvVec[0]);
        autil::StringUtil::trim(kvVec[1]);
        tagsMap[kvVec[0]] = kvVec[1];
    }
    return true;
}

navi::ErrorCode MetricsReporterR::init(navi::ResourceInitContext &ctx) {
    _reporter.reset(new kmonitor::MetricsReporter(_prefix, _path, _tags));
    return navi::EC_NONE;
}

const kmonitor::MetricsReporterPtr &MetricsReporterR::getReporter() const {
    return _reporter;
}

REGISTER_RESOURCE(MetricsReporterR);

}

