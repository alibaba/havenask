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
#ifndef MULTI_CALL_INTERFACE_LOADBALANCERCONTEXT_H_
#define MULTI_CALL_INTERFACE_LOADBALANCERCONTEXT_H_

#include <map>
#include <string>

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/config/MultiCallConfig.h"
#include "aios/network/gig/multi_call/interface/SearchService.h"
#include "aios/network/gig/multi_call/service/ProviderSelectorFactory.h"
#include "aios/network/gig/multi_call/service/VersionSelectorFactory.h"
#include "autil/Lock.h"

namespace multi_call {

class LoadBalancerContext
{
public:
    LoadBalancerContext(const SearchServicePtr &searchService)
        : _versionSelectorFactory(searchService->getVersionSelectorFactory())
        , _providerSelectorFactory(searchService->getProviderSelectorFactory()) {
    }
    virtual ~LoadBalancerContext() {
    }

public:
    VersionSelectorPtr getVersionSelector(const std::string &bizname);
    ProviderSelectorPtr getProviderSelector(const std::string &bizname);

private:
    autil::ThreadMutex _mutex;
    // bizname -> versionSelector
    std::map<std::string, VersionSelectorPtr> _versionSelectorMap;
    // bizname -> providerSelector
    std::map<std::string, ProviderSelectorPtr> _providerSelectorMap;

    VersionSelectorFactoryPtr _versionSelectorFactory;
    ProviderSelectorFactoryPtr _providerSelectorFactory;
};

MULTI_CALL_TYPEDEF_PTR(LoadBalancerContext);

} // namespace multi_call

#endif // MULTI_CALL_INTERFACE_LOADBALANCERCONTEXT_H_
