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
#include "navi/resource/GigClientResource.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/util/CommonUtil.h"

using namespace multi_call;
using namespace autil::legacy;

namespace navi {

GigClientResource::GigClientResource() {
}

GigClientResource::~GigClientResource() {
    size_t count = 0;
    if (_searchService) {
        CommonUtil::waitUseCount(_searchService, 1, 5 * 1000 * 1000);
    }
    NAVI_KERNEL_LOG(DEBUG,
                    "gig client resource destructed [%p], search "
                    "service [%p], use_count: [%lu], sleep count: %lu",
                    this, _searchService.get(), _searchService.use_count(),
                    count);
    if (_searchService.use_count() > 1) {
        NAVI_KERNEL_LOG(ERROR, "gig destruct bt: %s",
                        NaviLogger::getCurrentBtString().c_str());
    }
}

void GigClientResource::def(ResourceDefBuilder &builder) const {
    builder
        .name(GIG_CLIENT_RESOURCE_ID);
}

bool GigClientResource::config(navi::ResourceConfigContext &ctx) {
    ctx.Jsonize("init_config", _initConfig);
    ctx.Jsonize("flow_config", _flowConfigMap, _flowConfigMap);
    ctx.Jsonize("node_path", _nodePath, _nodePath);
    return true;
}

navi::ErrorCode GigClientResource::init(navi::ResourceInitContext &ctx) {
    _searchService.reset(new SearchService());
    NAVI_KERNEL_LOG(DEBUG, "[%p] search service created [%p]", this,
                    _searchService.get());
    if (!_searchService->init(_initConfig)) {
        NAVI_KERNEL_LOG(ERROR, "search service init failed [%p] config [%s]",
                        _searchService.get(), ToJsonString(_initConfig).c_str());
        return navi::EC_INIT_RESOURCE;
    }
    initFlowConfig();
    return navi::EC_NONE;
}

void GigClientResource::initFlowConfig() {
    std::map<std::string, FlowControlConfigPtr> flowControlConfigMap;
    for (const auto &pair : _flowConfigMap) {
        const auto &strategy = pair.first;
        const auto &config = pair.second;

        FlowControlConfigPtr configPtr(new FlowControlConfig(config));
        flowControlConfigMap.emplace(strategy, configPtr);
    }
    _searchService->updateFlowConfig(flowControlConfigMap);
}

REGISTER_RESOURCE(GigClientResource);

}

