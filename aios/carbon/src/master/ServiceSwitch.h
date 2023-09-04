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
#ifndef CARBON_SERVICESWITCH_H
#define CARBON_SERVICESWITCH_H

#include "common/common.h"
#include "carbon/Log.h"
#include "carbon/CommonDefine.h"
#include "carbon/Status.h"
#include "master/ServiceAdapter.h"
#include "master/ServiceAdapterCreator.h"
#include "autil/Lock.h"

BEGIN_CARBON_NAMESPACE(master);

class WorkerNode;
CARBON_TYPEDEF_PTR(WorkerNode);

typedef std::map<nodeid_t, ServiceInfo> ServiceInfoMap;

class ServiceSwitch
{
public:
    ServiceSwitch(const std::string &id,
                  const SerializerCreatorPtr &serializerCreatorPtr,
                  ServiceAdapterExtCreatorPtr extCreatorPtr = ServiceAdapterExtCreatorPtr());
    virtual ~ServiceSwitch();
private:
    ServiceSwitch(const ServiceSwitch &);
    ServiceSwitch& operator=(const ServiceSwitch &);
public:
    void update(const std::vector<WorkerNodePtr> &workerNodes);

    /*
      if the node not in the ServiceInfo map,
      it means the note is SVT_UNAVAILABLE
    */
    virtual std::map<nodeid_t, ServiceInfo> getServiceInfos() const;

    void updateConfigs(const std::vector<ServiceConfig> &serviceConfigs);

    bool recoverServiceAdapters(const std::vector<ServiceConfig> &serviceConfigs);

    bool isWorking() const { return _isWorking;}

public:
    void sync();

    std::string getId() { return _id; }

protected:
    /* virtual for mock */
    virtual void doUpdate(const std::vector<WorkerNodePtr> &workerNodes);

public: // for test
    void setServiceAdapterCreator(const ServiceAdapterCreatorPtr &adapterCreatorPtr) {
        _serviceAdapterCreatorPtr = adapterCreatorPtr;
    }

    std::map<std::string, ServiceAdapterPtr> getServiceAdapters() const {
        return _serviceAdapters;
    }

    std::map<nodeid_t, ServiceNode> getServiceNodes() const {
        return _serviceNodes;
    }

    void setServiceAdapters(const std::map<std::string, ServiceAdapterPtr> &adapters) {
        _serviceAdapters = adapters;
    }

    void setServiceNodes(const std::map<nodeid_t, ServiceNode> &serviceNodes) {
        _serviceNodes = serviceNodes;
        _serviceNodesAll = serviceNodes;
    }
    bool isCleared();
public: // public for test
    static ServiceType getServiceStatus(const ServiceNode &localNode,
            const std::map<std::string, ServiceNodeSet> &remoteServiceNodesVect);

private:
    void getServiceInfos(const std::map<nodeid_t, ServiceNode> &serviceNodes,
                         const std::map<std::string, ServiceNodeSet> &remoteServiceNodesVect,
                         std::map<nodeid_t, ServiceInfo> *serviceInfoMap);

    void removeUnPublishedAdapters();

    std::string genServiceAdapterId(const ServiceConfig &config);
private:
    std::string _id;
    std::map<std::string, ServiceAdapterPtr> _serviceAdapters;
    std::map<nodeid_t, ServiceNode> _serviceNodes;
    std::map<nodeid_t, ServiceNode> _serviceNodesAll;
    std::map<nodeid_t, ServiceInfo> _serviceInfos;
    std::map<std::string, ServiceNodeSet> _remoteServiceNodesMap;
    ServiceAdapterCreatorPtr _serviceAdapterCreatorPtr;
    bool _updated;
    bool _isWorking;
    bool _succeed;
    mutable autil::ThreadMutex _lock;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(ServiceSwitch);

END_CARBON_NAMESPACE(master);

#endif //CARBON_SERVICESWITCH_H
