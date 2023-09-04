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
#include "master/ServiceNodeManager.h"
#include "carbon/Log.h"

using namespace std;
using namespace autil;
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, ServiceNodeManager);

ServiceNodeManager::ServiceNodeManager(const SerializerPtr &serializerPtr) {
    _serializerPtr = serializerPtr;
}

ServiceNodeManager::~ServiceNodeManager() {
}

bool ServiceNodeManager::addNodes(const ServiceNodeMap &serviceNodes)
{
    ScopedLock lock(_lock);
    CARBON_LOG(DEBUG, "serviceNodes:%s.",
               autil::legacy::ToJsonString(serviceNodes, true).c_str());
    ServiceNodeMap tmpServiceNodes = _serviceNodes;
    tmpServiceNodes.insert(serviceNodes.begin(), serviceNodes.end());
    return persist(tmpServiceNodes);
}
    
bool ServiceNodeManager::delNodes(const ServiceNodeMap &serviceNodes) {
    ScopedLock lock(_lock);    
    CARBON_LOG(DEBUG, "serviceNodes:%s.",
               autil::legacy::ToJsonString(serviceNodes, true).c_str());
    
    ServiceNodeMap tmpServiceNodes = _serviceNodes;
    for (ServiceNodeMap::const_iterator it = serviceNodes.begin();
         it != serviceNodes.end(); it++)
    {
        tmpServiceNodes.erase(it->first);
    }

    return persist(tmpServiceNodes);
}
    
const ServiceNodeMap& ServiceNodeManager::getNodes() const {
    ScopedLock lock(_lock);    
    return _serviceNodes;
}

bool ServiceNodeManager::recover() {
    ScopedLock lock(_lock);    
    bool bExist = false;
    if (!_serializerPtr->checkExist(bExist)) {
        CARBON_LOG(ERROR, "check service node manager serialized file failed.");
        return false;
    }

    if (!bExist) {
        CARBON_LOG(INFO, "recover service node map success.");
        return true;
    }
    
    string str;
    if (!_serializerPtr->read(str)) {
        CARBON_LOG(ERROR, "read from zk failed.");
        return false;
    }
    CARBON_LOG(DEBUG, "service node manager json str:%s.", str.c_str());

    try {
        autil::legacy::FromJsonString(_serviceNodes, str);
    } catch (const autil::legacy::ExceptionBase &e) {
        CARBON_LOG(ERROR, "from json str failed, exception:[%s].", e.what());
        return false;
    }
    
    CARBON_LOG(INFO, "recover service node map success. service nodes count:[%zd].",
           _serviceNodes.size());
    return true;
}

bool ServiceNodeManager::persist(const ServiceNodeMap &serviceNodes) {
    CARBON_LOG(DEBUG, "persist service nodes:[%s], cur nodes:[%s].",
               autil::legacy::ToJsonString(serviceNodes, true).c_str(),
               autil::legacy::ToJsonString(_serviceNodes, true).c_str());
    
    if (serviceNodes == _serviceNodes) {
        CARBON_LOG(DEBUG, "service nodes not changed.");
        return true;
    }
    
    string str;
    try {
        str = autil::legacy::ToJsonString(serviceNodes, true);
    } catch (const autil::legacy::ExceptionBase &e) {
        CARBON_LOG(ERROR, "serialize the group service node map to "
               "json str fail, exception:[%s].", e.what());
        return false;
    }

    if (!_serializerPtr->write(str)) {
        CARBON_LOG(ERROR, "persist the group service node map failed.");
        return false;
    }

    _serviceNodes = serviceNodes;
    CARBON_LOG(INFO, "service node manager success, nodes count: %zd, "
           "json str:%s.", _serviceNodes.size(), str.c_str());
    return true;
}

END_CARBON_NAMESPACE(master);

