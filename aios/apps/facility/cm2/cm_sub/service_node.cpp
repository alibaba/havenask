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
#include "aios/apps/facility/cm2/cm_sub/service_node.h"

#include <stdio.h>

#include "aios/apps/facility/cm2/cm_sub/topo_cluster.h"

namespace cm_sub {

ServiceNode::ServiceNode()
{
    _groupId = 0;
    memset(_ip, 0, IP_LEN);
}

ServiceNode::ServiceNode(const std::shared_ptr<TopoNode>& topoNode, cm_basic::ProtocolType protocol,
                         cm_basic::NodeStatus status)
{
    _version = topoNode->getVersion();
    auto cmNode = topoNode->getNode();
    if (!cmNode) {
        return;
    }

    // _groupId
    _groupId = cmNode->getGroupID();
    // _ip
    snprintf(_ip, IP_LEN, "%s", cmNode->getNodeIP().c_str());
    // _protoPort 如果对协议类型有要求，需要看该节点是否包含该协议
    int32_t cnt = cmNode->getProtoPortSize();
    for (int32_t i = 0; i < cnt; ++i) {
        if (protocol == cm_basic::PT_ANY || cmNode->getProtoType(i) == protocol) {
            // 注意这里 protocol == cm_basic::PT_ANY情况，只取了第一个
            _protoPort.CopyFrom(cmNode->getProto(i));
            break;
        }
    }
    // _status
    _nodeStatus = status == cm_basic::NS_UNINIT ? cmNode->getCurStatus() : status;
    _onlineStatus = cmNode->getOnlineStatus();
    // _idcType
    if (cmNode->hasIDCType()) {
        _idcType = cmNode->getIDCType();
    } else {
        _idcType = cm_basic::IDC_ANY;
    }
    // _metaInfo, safe to read because update thread don't write it
    if (cmNode->hasMetaInfo()) {
        _metaInfo.CopyFrom(cmNode->getMetaInfo());
    }
}

ServiceNode::~ServiceNode() {}

} // namespace cm_sub
