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
#include "navi/resource/GigClientR.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/util/CommonUtil.h"

using namespace multi_call;

namespace navi {

const std::string GigClientR::RESOURCE_ID = "navi.buildin.gig_client";

GigClientR::GigClientR() {
}

GigClientR::~GigClientR() {
    size_t count = 0;
    if (_searchService) {
        _searchService->stealSnapshotChangeCallback();
        CommonUtil::waitUseCount(_searchService, 1, 5 * 1000 * 1000);
    }
    NAVI_KERNEL_LOG(DEBUG,
                    "gig client resource destructed [%p], search "
                    "service [%p], use_count: [%lu], sleep count: %lu",
                    this, _searchService.get(), _searchService.use_count(),
                    count);
    if (_searchService.use_count() > 1) {
        NAVI_KERNEL_LOG(
            ERROR, "gig destruct bt: %s",
            navi::NAVI_TLS_LOGGER->logger->getCurrentBtString().c_str());
    }
}

void GigClientR::def(ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, RS_SNAPSHOT)
        .produce(GigMetaR::RESOURCE_ID);
}

bool GigClientR::config(navi::ResourceConfigContext &ctx) {
    ctx.Jsonize("init_config", _initConfig, _initConfig);
    ctx.Jsonize("flow_config", _flowConfigMap, _flowConfigMap);
    ctx.Jsonize("node_path", _nodePath, _nodePath);
    return true;
}

navi::ErrorCode GigClientR::init(navi::ResourceInitContext &ctx) {
    if (NAVI_TLS_LOGGER) {
        _logger = *NAVI_TLS_LOGGER;
    }
    _notifier = ctx.getProduceNotifier();
    _searchService = std::make_shared<SearchService>();
    NAVI_KERNEL_LOG(DEBUG, "[%p] search service created [%p]", this,
                    _searchService.get());
    if (!_searchService->init(_initConfig)) {
        NAVI_KERNEL_LOG(ERROR, "search service init failed [%p] config [%s]",
                        _searchService.get(), ToJsonString(_initConfig).c_str());
        return navi::EC_INIT_RESOURCE;
    }
    initFlowConfig();
    if (_notifier) {
        _searchService->setSnapshotChangeCallback(this);
    }
    return navi::EC_NONE;
}

void GigClientR::initFlowConfig() {
    std::map<std::string, FlowControlConfigPtr> flowControlConfigMap;
    for (const auto &pair : _flowConfigMap) {
        const auto &strategy = pair.first;
        const auto &config = pair.second;

        FlowControlConfigPtr configPtr(new FlowControlConfig(config));
        flowControlConfigMap.emplace(strategy, configPtr);
    }
    _searchService->updateFlowConfig(flowControlConfigMap);
}

void GigClientR::Run() {
    if (!_notifier) {
        return;
    }
    NaviLoggerScope scope(_logger);
    std::vector<BizMetaInfo> bizMetaInfos;
    if (!_searchService->getBizMetaInfos(bizMetaInfos)) {
        NAVI_LOG(ERROR, "getBizMetaInfos failed");
        return;
    }
    auto metaR = std::make_shared<GigMetaR>(bizMetaInfos);
    if (_metaR && _metaR->isSame(metaR)) {
        return;
    }
    metaR->log();
    _metaR = metaR;
    NAVI_LOG(INFO, "create GigMetaR [%p]", metaR.get());
    _notifier->notify(metaR);
}

REGISTER_RESOURCE(GigClientR);

}
