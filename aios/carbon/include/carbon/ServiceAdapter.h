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
#ifndef CARBON_SERVICE_ADAPTER_H
#define CARBON_SERVICE_ADAPTER_H

#include "CommonDefine.h"
#include "Log.h"
#include "RolePlan.h"
#include "autil/legacy/jsonizable.h"
#include <memory>

namespace carbon {

enum ServiceNodeStatus {
    SN_UNKNOWN = 0,
    SN_AVAILABLE = 1,
    SN_UNAVAILABLE = 2,
    SN_DISABLED = 3
};

struct ServiceNode : public autil::legacy::Jsonizable {
    ServiceNode() {
        status = SN_UNKNOWN;
        score = 0;
    }

    bool operator < (const ServiceNode &rhs) const {
        return ip < rhs.ip;
    }

    bool operator == (const ServiceNode &rhs) const {
        return ip == rhs.ip;
    }

    JSONIZE() {
        JSON_FIELD(ip);
        JSON_FIELD(nodeId);
    }

    nodeid_t nodeId;
    std::string ip;
    ServiceNodeStatus status;
    KVMap metas;
    int score;
};

typedef std::set<ServiceNode> ServiceNodeSet;
typedef std::map<nodespec_t, ServiceNode> ServiceNodeMap;

enum ServiceAdapterStatus {
    SS_PUBLISHED = 0,
    SS_UNPUBLISHING = 1,
    SS_UNPUBLISHED = 2,
};

class ServiceAdapter
{
public:
    ServiceAdapter(const std::string &id = "");
    virtual ~ServiceAdapter();
    
private:
    ServiceAdapter(const ServiceAdapter &);
    ServiceAdapter& operator=(const ServiceAdapter &);
    
public:
    virtual bool doInit(const ServiceConfig &serviceConfig) = 0;

    virtual ServiceAdapterType getType() const = 0;

    virtual void doUpdateConfig(const ServiceConfig &serviceConfig) {}
    
public:
    bool sync(const ServiceNodeSet &localNodes, ServiceNodeSet *remoteNodes);

    void unPublish();

    bool init(const ServiceConfig &serviceConfig) {
        _config = serviceConfig;
        return doInit(serviceConfig);
    }

    void updateConfig(const ServiceConfig &serviceConfig) {
        _config = serviceConfig;
        doUpdateConfig(serviceConfig);
    }
    
    /* virtual for mock */
    virtual bool isUnPublished();

    virtual bool recover() {return true;}

    virtual bool canSync() {return true;}
protected:
    virtual nodespec_t getServiceNodeSpec(const ServiceNode &node) const;
    
    virtual bool doGetNodes(ServiceNodeMap *nodes) = 0;

    virtual bool doUpdateNodes(const ServiceNodeMap &nodes) = 0;
    
    virtual bool doAddNodes(const ServiceNodeMap &nodes) = 0;

    virtual bool doDelNodes(const ServiceNodeMap &nodes) = 0;

private:
    void diffNodes(const ServiceNodeMap &localNodes,
                   const ServiceNodeMap &remoteNodes,
                   ServiceNodeMap *needAddNodes,
                   ServiceNodeMap *needDelNodes,
                   ServiceNodeMap *needUpdateNodes);

    bool getNodes(ServiceNodeMap *nodes);

    void updateNodes(const ServiceNodeMap &nodes);
    
    void addNodes(const ServiceNodeMap &nodes);

    void delNodes(const ServiceNodeMap &nodes);

    void extractNodes(const ServiceNodeMap &remoteNodesMap,
                      ServiceNodeSet *remoteNodes);

    void arrangeNodes(const ServiceNodeSet &localNodes,
                      ServiceNodeMap *localNodesMap);

    void clearDelayDeletedNodes(const ServiceNodeMap &remoteServiceNodes);

protected:
    void intersection(const ServiceNodeMap &local,
                      const ServiceNodeMap &remote,
                      ServiceNodeMap *result);
public: // for test
    bool getPublishSwitchFlag() const { return _publishSwitch; }

    ServiceAdapterStatus getStatus() const { return _status; }

protected:    
    std::string _id;
private:
    bool _publishSwitch;
    bool _hasGetNode;
    ServiceAdapterStatus _status;
    ServiceConfig _config;
    std::map<nodespec_t, int64_t> _firstDeleteTime;
private:
    CARBON_LOG_DECLARE();
};

typedef std::shared_ptr<ServiceAdapter> ServiceAdapterPtr;

}
#endif
