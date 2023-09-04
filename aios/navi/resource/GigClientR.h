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
#ifndef NAVI_GIG_CLIENT_RESOURCE_H
#define NAVI_GIG_CLIENT_RESOURCE_H

#include <multi_call/interface/SearchService.h>
#include "navi/engine/Resource.h"
#include "navi/resource/GigMetaR.h"

namespace navi {

class GigClientR : public navi::Resource, public multi_call::SnapshotChangeCallback
{
public:
    GigClientR();
    ~GigClientR();
public:
    GigClientR(const GigClientR &) = delete;
    GigClientR &operator=(const GigClientR &) = delete;
public:
    void def(ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;
public:
    static const std::string RESOURCE_ID;
public:
    // gig callback
    void Run() override;
private:
    void initFlowConfig();
public:
    const multi_call::SearchServicePtr &getSearchService() const {
        return _searchService;
    }
private:
    DECLARE_LOGGER();
    multi_call::MultiCallConfig _initConfig;
    std::map<std::string, multi_call::FlowControlConfig> _flowConfigMap;
    std::string _nodePath;
    std::string _metricPath;
    multi_call::SearchServicePtr _searchService;
    ProduceNotifierPtr _notifier;
    GigMetaRPtr _metaR;
};

}

#endif //NAVI_GIG_CLIENT_RESOURCE_H
