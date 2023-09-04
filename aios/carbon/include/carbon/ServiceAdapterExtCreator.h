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
#ifndef CARBON_SERVICE_ADAPTER_EXT_CREATOR_H
#define CARBON_SERVICE_ADAPTER_EXT_CREATOR_H

#include "CommonDefine.h"
#include "ServiceAdapter.h"
#include <memory>
#include <string>

namespace carbon {

class ServiceAdapterExtCreator 
{
public:
    virtual ~ServiceAdapterExtCreator() {}
    virtual ServiceAdapterPtr create(const std::string &serviceAdapterId, const ServiceConfig &config) = 0;
};

typedef std::shared_ptr<ServiceAdapterExtCreator> ServiceAdapterExtCreatorPtr;

}

#endif //CARBON_SERVICE_ADAPTER_EXT_CREATOR_H
