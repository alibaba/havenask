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
/**
 * =====================================================================================
 *
 *       Filename:  service_node.h
 *
 *    Description:  查询接口返回的服务节点
 *
 *        Version:  0.1
 *        Created:  2012-08-07 15:11:09
 *       Revision:  none
 *       Compiler:  glibc
 *
 *         Author:  tiechou (search engine group), tiechou@taobao.com
 *        Company:  www.taobao.com
 *
 * =====================================================================================
 */

#ifndef __SERVICE_NODE_H_
#define __SERVICE_NODE_H_

#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/cm_common.pb.h"
#include "aios/apps/facility/cm2/cm_basic/common/common.h"

namespace cm_sub {

class TopoNode;

/**
 * @class ServiceNode
 * @brief 查询返回的服务节点
 */
class ServiceNode
{
public:
    int32_t _groupId;                     ///< 列号
    char _ip[cm_basic::IP_LEN];           ///< ip
    cm_basic::ProtocolPort _protoPort;    ///< 协议和端口
    cm_basic::NodeStatus _nodeStatus;     ///< 节点的存活状态
    cm_basic::OnlineStatus _onlineStatus; ///< 节点的在线状态
    cm_basic::IDCType _idcType;           ///< 节点所在机房
    cm_basic::NodeMetaInfo _metaInfo;     ///< 节点元信息
    long _version;

public:
    ServiceNode();
    /**
     * @brief   构造函数
     * @param   [in]    topo_node 拓扑节点
     * @param   [in]    protocol 需要的协议类型
     */
    ServiceNode(const std::shared_ptr<TopoNode>& topo_node, cm_basic::ProtocolType protocol,
                cm_basic::NodeStatus status = cm_basic::NS_UNINIT); // to fix status changed bug
    ~ServiceNode();
};

} // namespace cm_sub
#endif
