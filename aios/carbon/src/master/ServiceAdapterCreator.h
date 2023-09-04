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
#ifndef CARBON_SERVICEADAPTERCREATOR_H
#define CARBON_SERVICEADAPTERCREATOR_H

#include "common/common.h"
#include "carbon/Log.h"
#include "carbon/ServiceAdapterExtCreator.h"
#include "master/ServiceAdapter.h"
#include "master/ServiceNodeManager.h"
#include "master/SerializerCreator.h"

BEGIN_CARBON_NAMESPACE(master);

class ServiceAdapterCreator
{
public:
    ServiceAdapterCreator(const SerializerCreatorPtr &serializerCreatorPtr,
            ServiceAdapterExtCreatorPtr extCreatorPtr = ServiceAdapterExtCreatorPtr());
    virtual ~ServiceAdapterCreator();
private:
    ServiceAdapterCreator(const ServiceAdapterCreator &);
    ServiceAdapterCreator& operator=(const ServiceAdapterCreator &);
public:
    virtual ServiceAdapterPtr create(
            const std::string &serviceAdapterId,
            const ServiceConfig &config);

private:
    ServiceNodeManagerPtr genServiceNodeManager(
            const std::string &serviceAdapterId,
            const ServiceConfig &config);

    ServiceAdapterPtr doCreate(
            const std::string &serviceAdapterId,
            const ServiceConfig &config);

private:
    SerializerCreatorPtr _serializerCreatorPtr;
    ServiceAdapterExtCreatorPtr _extCreatorPtr;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(ServiceAdapterCreator);

END_CARBON_NAMESPACE(master);

#endif //CARBON_SERVICEADAPTERCREATOR_H
