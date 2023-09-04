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
#ifndef CARBON_CM2SERVICEADAPTER_H
#define CARBON_CM2SERVICEADAPTER_H

#include "common/common.h"
#include "carbon/Log.h"
#include "master/ServiceAdapter.h"
#include "master/ServiceNodeManager.h"
#include "aios/apps/facility/cm2/cm_server/proto/cm_ctrl.pb.h"
#include "aios/apps/facility/cm2/cm_basic/leader_election/master_apply.h"

#include "aios/network/arpc/arpc/ANetRPCChannel.h"
#include "aios/network/arpc/arpc/ANetRPCChannelManager.h"

BEGIN_CARBON_NAMESPACE(master);

struct CM2ServiceConfig : public autil::legacy::Jsonizable
{
public:
    CM2ServiceConfig() {
        tcpPort = 0;
        httpPort = 0;
        idcType = (int32_t)cm_basic::IDC_ANY;
        weight = 100;
    }

    ~CM2ServiceConfig() {}
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        json.Jsonize("cm2_server_zookeeper_host", zkHost, std::string());
        json.Jsonize("cm2_server_leader_path", leaderPath, std::string());
        json.Jsonize("cm2_server_cluster_name", clusterName, std::string());
        json.Jsonize("tcpPort", tcpPort, 0);
        json.Jsonize("httpPort", httpPort, 0);
        json.Jsonize("idc_type", idcType, 0);
        json.Jsonize("weight", weight, 100);
    }
public:
    std::string zkHost;
    std::string leaderPath;
    std::string clusterName;
    int32_t tcpPort;
    int32_t httpPort;
    int32_t idcType;
    int32_t weight;
};

class CM2ServiceAdapter : public ServiceAdapter
{
    typedef std::map<std::string, cm_basic::CMNode> CMNodeMap;
public:
    CM2ServiceAdapter(const std::string &id = "");
    ~CM2ServiceAdapter();
private:
    CM2ServiceAdapter(const CM2ServiceAdapter &);
    CM2ServiceAdapter& operator=(const CM2ServiceAdapter &);
public:
    /* override */
    ServiceAdapterType getType() const {
        return ST_CM2;
    }

    std::string getServiceConfigStr() const {
        return autil::legacy::ToJsonString(_cm2ServiceConfig);
    }

    /* override */
    bool doInit(const ServiceConfig &serviceConfig);

    /* override */
    void doUpdateConfig(const ServiceConfig &serviceConfig);
protected:
    /* override */
    nodespec_t getServiceNodeSpec(const ServiceNode &node) const;

    /* override */
    bool doAddNodes(const ServiceNodeMap &nodes);

    /* override */
    bool doDelNodes(const ServiceNodeMap &nodes);

    /* override */
    bool doGetNodes(ServiceNodeMap *nodes);

    /* override */
    bool doUpdateNodes(const ServiceNodeMap &nodes);

private:
    bool operateNodes(const ServiceNodeMap &nodes,
                      cm_server::CMD_TYPE type);

    bool doOperateNodes(const std::vector<cm_basic::CMNode> &cmNodes,
                        cm_server::CMD_TYPE type);

    bool needUpdate(const cm_basic::CMNode &preNode, const cm_basic::CMNode &curNode);

    ServiceNode toServiceNode(const cm_basic::CMNode &node);

    cm_basic::CMNode toCMNode(const ServiceNode &node);

    void fillByMetaInfo(const KVMap &metas, cm_basic::CMNode *cmNode);

    /* virtual for test */
    virtual cm_server::CMCmdProcService* getServiceStub();

    arpc::ANetRPCChannel* getRpcChannel();

    bool getLeader(std::string *leaderSpec);

    bool checkUniqueSignature(const cm_basic::CMNode &node) const;
public:
    void setServiceConifg(CM2ServiceConfig cm2ServiceConifg) {
        _cm2ServiceConfig = cm2ServiceConifg;
    }

     CM2ServiceConfig getServiceConifg() {
         return _cm2ServiceConfig;
    }

    std::string getZkLeaderPath() {
        return _zkLeaderPath;
    }

    cm_basic::MasterApply* getMasterApply() {
        return _masterApply;
    }
    arpc::ANetRPCChannelManager* getRpcChannelManager() {
        return _rpcChannelManager;
    }

    std::string getSpec(const std::string &ip,
                        int32_t tcpPort, int32_t httpPort) const;

    nodespec_t getCMNodeSpec(const cm_basic::CMNode &node);

public:// public for test
    void setLastCMNodeMap(const CMNodeMap &cmNodeMaps) {
        _lastCMNodeMap = cmNodeMaps;
    }

    CMNodeMap getLastCMNodeMap() {
        return _lastCMNodeMap;
    }

private:
    CM2ServiceConfig _cm2ServiceConfig;
    CMNodeMap _lastCMNodeMap;
    std::string _zkLeaderPath;
    cm_basic::MasterApply *_masterApply;
    arpc::ANetRPCChannelManager *_rpcChannelManager;
    arpc::ANetRPCChannel *_rpcChannel;
    std::string _serviceSignature;
    std::string _metaStr; // guarded by _lock
    mutable autil::ThreadMutex _lock;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(CM2ServiceAdapter);

END_CARBON_NAMESPACE(master);

#endif //CARBON_CM2SERVICEADAPTER_H
