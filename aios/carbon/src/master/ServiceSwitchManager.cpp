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
#include "master/ServiceSwitchManager.h"
#include "carbon/Log.h"

using namespace std;
using namespace autil;
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, ServiceSwitchManager);

ServiceSwitchManager::ServiceSwitchManager(
         const SerializerCreatorPtr &serializerCreatorPtr, ServiceAdapterExtCreatorPtr extCreator)
{
    _serializerCreatorPtr = serializerCreatorPtr;
     _extCreatorPtr = extCreator;
}

ServiceSwitchManager::~ServiceSwitchManager() {
    _workLoopThreadPtr.reset();
}

bool ServiceSwitchManager::start() {
    _workLoopThreadPtr = LoopThread::createLoopThread(
            std::bind(&ServiceSwitchManager::sync, this),
            SERVICE_SYNC_INTERVAL);
    if (_workLoopThreadPtr == NULL) {
        CARBON_LOG(ERROR, "create service sync work loop thread failed.");
        return false;
    }
    if (!_runner.start()) {
        CARBON_LOG(ERROR, "start runner for serviceSwitchManager.");
        return false;
    }

    CARBON_LOG(INFO, "start ServiceSwitchManager success.");
    return true;
}

ServiceSwitchPtr ServiceSwitchManager::getServiceSwitch(
        const roleguid_t &roleGUID)
{
    ScopedLock lock(_lock);
    map<roleguid_t, ServiceSwitchPtr>::iterator it = _serviceSwitchMap.find(roleGUID);
    if (it != _serviceSwitchMap.end()) {
        return it->second;
    }

    ServiceSwitchPtr serviceSwitchPtr(
            new ServiceSwitch(roleGUID, _serializerCreatorPtr, _extCreatorPtr));
    _serviceSwitchMap[roleGUID] = serviceSwitchPtr;

    return serviceSwitchPtr;
}

void ServiceSwitchManager::sync() {
    map<roleguid_t, ServiceSwitchPtr> serviceSwitchMap;
    {
        ScopedLock lock(_lock);
        auto it = _serviceSwitchMap.begin();
        for (; it != _serviceSwitchMap.end(); it++) {
            if (!it->second.unique() || !it->second->isCleared()) {
                serviceSwitchMap[it->first] = it->second;
            }
        }
        _serviceSwitchMap = serviceSwitchMap;
    }
    vector<std::function<void()>> funcs;
    for (const auto &serviceSwitch : serviceSwitchMap) {
        funcs.push_back([serviceSwitch](){serviceSwitch.second->sync();});
    }
    _runner.Run(funcs);
}

END_CARBON_NAMESPACE(master);
