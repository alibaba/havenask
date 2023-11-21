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
#ifndef __CM_NODE_WRAPPER_
#define __CM_NODE_WRAPPER_

#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/cm_node.pb.h"
#include "aios/apps/facility/cm2/cm_basic/common/common.h"
#include "autil/Lock.h"

namespace cm_basic {

class CMNodeWrapper
{
public:
    CMNodeWrapper(const std::string& node_spec, CMNode* node);
    virtual ~CMNodeWrapper();

private:
    CMNodeWrapper(const CMNodeWrapper&);
    CMNodeWrapper& operator=(const CMNodeWrapper&);

public:
    CMNode* cloneNode();

public:
    void setOnlineStatus(OnlineStatus online_status);
    void setCurStatus(NodeStatus node_status);
    void setPrevStatus(NodeStatus node_status);
    void setLBInfo(const NodeLBInfo& lb_info);
    void setLBInfoWeight(int32_t weight);
    void setHeartbeatTime(int64_t hb_time);
    void setValidTime(int64_t valid_time);
    void setStartTime(int64_t start_time);
    void setMetaInfo(const NodeMetaInfo& meta_info);
    void clearTopoInfo();
    void setTopoInfo(const std::string& topo_info);
    void setIDCType(IDCType idcType);
    void addProtoPort(ProtocolType protocol, int32_t port);
    void setNodeIP(const std::string& node_ip);
    void setClusterName(const std::string& cluster_name);

public:
    std::string getNodeIP();
    std::string getClusterName();
    std::string getNodeSpec();

    int32_t getGroupID();
    int64_t getValidTime();
    int64_t getHeartbeatTime();
    int64_t getStartTime();

    OnlineStatus getOnlineStatus();
    NodeStatus getCurStatus();
    NodeStatus getPrevStatus();

    bool hasIDCType();
    IDCType getIDCType();

    bool hasTopoInfo();
    std::string getTopoInfo();
    const std::string& getTopoInfoRef();

    bool hasLBInfo();
    bool hasLBInfoWeight();
    NodeLBInfo getLBInfo();
    int32_t getLBInfoWeight();

    bool hasMetaInfo();
    NodeMetaInfo getMetaInfo();

    int32_t getProtoPortSize();
    ProtocolType getProtoType(int i);
    int32_t getProtoPort(int i);
    ProtocolPort getProto(int i);

public:
    static void makeSpec(const CMNode* cm_node, std::string& node_spec);

public:
    void CopyTo(CMNode* node);

protected:
    autil::ReadWriteLock _rwlock;
    std::string _node_spec;
    CMNode* _cm_node;
};

typedef std::shared_ptr<CMNodeWrapper> CMNodeWrapperPtr;

} // namespace cm_basic

#endif //__CM_NODE_WRAPPER_