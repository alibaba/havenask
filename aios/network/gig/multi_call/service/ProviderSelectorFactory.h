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
#ifndef ISEARCH_MULTI_CALL_PROVIDER_SELECTOR_FACTORY_H
#define ISEARCH_MULTI_CALL_PROVIDER_SELECTOR_FACTORY_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/config/MultiCallConfig.h"
#include "aios/network/gig/multi_call/service/ProviderSelector.h"
#include "aios/network/gig/multi_call/service/VersionSelector.h"

namespace multi_call {

class ProviderSelectorFactory
{
public:
    ProviderSelectorFactory() {
    }
    virtual ~ProviderSelectorFactory() = default;

private:
    ProviderSelectorFactory(const ProviderSelectorFactory &) = delete;
    ProviderSelectorFactory &operator=(const ProviderSelectorFactory &) = delete;

public:
    ProviderSelectorPtr create(const std::string &bizName);

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(ProviderSelectorFactory);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_PROVIDER_SELECTOR_FACTORY_H
