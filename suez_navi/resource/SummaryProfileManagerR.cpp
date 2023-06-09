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
#include "suez_navi/resource/SummaryProfileManagerR.h"
#include "build_service/config/ResourceReader.h"
#include "ha3/config/HitSummarySchema.h"
#include "ha3/summary/SummaryProfileManagerCreator.h"

namespace suez_navi {

const std::string SummaryProfileManagerR::RESOURCE_ID = "summary_profile_manager_r";

SummaryProfileManagerR::SummaryProfileManagerR()
    : _tableInfoR(nullptr)
    , _cavaPluginManagerR(nullptr)
    , _metricReporterR(nullptr)
{
}

SummaryProfileManagerR::~SummaryProfileManagerR() {
}

void SummaryProfileManagerR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID)
        .depend(TableInfoR::RESOURCE_ID, true, BIND_RESOURCE_TO(_tableInfoR))
        .depend(CavaPluginManagerR::RESOURCE_ID, true,
                BIND_RESOURCE_TO(_cavaPluginManagerR))
        .depend(MetricsReporterR::RESOURCE_ID, true,
                BIND_RESOURCE_TO(_metricReporterR));
}

bool SummaryProfileManagerR::config(navi::ResourceConfigContext &ctx) {
    ctx.Jsonize("config_path", _configPath, _configPath);
    ctx.Jsonize("summary_profile", _summaryProfileConfig);
    return true;
}

navi::ErrorCode SummaryProfileManagerR::init(navi::ResourceInitContext &ctx) {
    const auto &tableInfoPtr = _tableInfoR->getTableInfoWithRel();
    if (!tableInfoPtr) {
        NAVI_KERNEL_LOG(ERROR, "get table info failed");
        return navi::EC_ABORT;
    }
    build_service::config::ResourceReaderPtr resourceReader(
        new build_service::config::ResourceReader(_configPath));
    isearch::config::HitSummarySchemaPtr hitSummarySchemaPtr(
        new isearch::config::HitSummarySchema(tableInfoPtr.get()));
    isearch::summary::SummaryProfileManagerCreator summaryCreator(
        resourceReader, hitSummarySchemaPtr,
        _cavaPluginManagerR->getManager().get(),
        _metricReporterR->getReporter().get());
    auto manager = summaryCreator.create(_summaryProfileConfig);
    if (!manager) {
        return navi::EC_ABORT;
    }
    _summaryProfileManager = manager;
    return navi::EC_NONE;
}

isearch::summary::SummaryProfileManagerPtr SummaryProfileManagerR::getManager() const {
    return _summaryProfileManager;
}

REGISTER_RESOURCE(SummaryProfileManagerR);

}

