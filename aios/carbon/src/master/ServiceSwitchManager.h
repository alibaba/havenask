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
#ifndef CARBON_SERVICESWITCHMANAGER_H
#define CARBON_SERVICESWITCHMANAGER_H

#include "common/common.h"
#include "common/MultiThreadRunner.h"
#include "carbon/Log.h"
#include "master/ServiceSwitch.h"
#include "master/SerializerCreator.h"
#include "autil/Lock.h"
#include "autil/LoopThread.h"

BEGIN_CARBON_NAMESPACE(master);

class ServiceSwitchManager
{
public:
    ServiceSwitchManager(const SerializerCreatorPtr &serializerCreatorPtr,
            ServiceAdapterExtCreatorPtr creator = ServiceAdapterExtCreatorPtr());
    virtual ~ServiceSwitchManager();
private:
    ServiceSwitchManager(const ServiceSwitchManager &);
    ServiceSwitchManager& operator=(const ServiceSwitchManager &);
public:
    bool start();

    virtual ServiceSwitchPtr getServiceSwitch(const roleguid_t &roleGUID);

private:
    void sync();

private:
    std::map<roleguid_t, ServiceSwitchPtr> _serviceSwitchMap;
    SerializerCreatorPtr _serializerCreatorPtr;
    autil::ThreadMutex _lock;

    common::MultiThreadRunner _runner;
    autil::LoopThreadPtr _workLoopThreadPtr;

    ServiceAdapterExtCreatorPtr _extCreatorPtr;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(ServiceSwitchManager);

END_CARBON_NAMESPACE(master);

#endif //CARBON_SERVICESWITCHMANAGER_H
