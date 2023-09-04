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
#ifndef CARBON_SERVICENODEMANAGER_H
#define CARBON_SERVICENODEMANAGER_H

#include "common/common.h"
#include "carbon/Log.h"
#include "master/ServiceAdapter.h"
#include "master/Serializer.h"
#include "autil/Lock.h"

BEGIN_CARBON_NAMESPACE(master);

class ServiceNodeManager
{
public:
    ServiceNodeManager(const SerializerPtr &serializerPtr);
    virtual ~ServiceNodeManager();
private:
    ServiceNodeManager(const ServiceNodeManager &);
    ServiceNodeManager& operator=(const ServiceNodeManager &);
    
public:
    /* virtual for mock */
    virtual bool addNodes(const ServiceNodeMap &serviceNodes);

    /* virtual for mock */
    virtual bool delNodes(const ServiceNodeMap &serviceNodes);
    
    /* virtual for mock */
    virtual const ServiceNodeMap& getNodes() const;

    bool recover();

private:
    bool persist(const ServiceNodeMap &serviceNodes);
    
private:
    SerializerPtr _serializerPtr;
    ServiceNodeMap _serviceNodes;
    mutable autil::ThreadMutex _lock;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(ServiceNodeManager);

END_CARBON_NAMESPACE(master);

#endif //CARBON_SERVICENODEMANAGER_H
