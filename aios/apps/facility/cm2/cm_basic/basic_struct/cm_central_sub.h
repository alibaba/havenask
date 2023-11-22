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
/*********************************************************************
 * $Author: liang.xial $
 *
 * $LastChangedBy: liang.xial $
 *
 * $Revision: 2577 $
 *
 * $LastChangedDate: 2011-03-09 09:50:55 +0800 (Wed, 09 Mar 2011) $
 *
 * $Id: cm_central_sub.h 2577 2011-03-09 01:50:55Z liang.xial $
 *
 * $Brief: ini file parser $
 ********************************************************************/

#ifndef CM_SUB_CM_CENTRAL_SUB_H_
#define CM_SUB_CM_CENTRAL_SUB_H_
#include "aios/apps/facility/cm2/cm_basic/basic_struct/cm_central.h"
namespace cm_basic {

class CMCentralSub : public virtual CMCentral
{
public:
    CMCentralSub() {}
    virtual ~CMCentralSub() {}
    /*
     * @brief   更新集群状态数组
     * @param   [in]    集群名
     * @param   [in]    集群状态数组
     * @return  0:成功； -1:失败
     */
    int32_t updateClusterNodeStatus(const std::string& cluster_name, NodeStatusVec* status_vec);
    /*
     * @brief   更新一个 CMCluster, 把更新后的CMCluster加入到_mapNameToCluster中管理
     * @param   [in]   集群指针，外边申请。
     * @return  RSP_SUCCESS:成功； 失败，外边需要删除p_cm_cluster
     */
    RespStatus updateCluster(CMCluster* p_cm_cluster);
    /**
     * @brief 得到一个集群的offline/online状态
     * @param cluster_name[in] 集群名
     * @return 集群状态，如果集群名不存在，也返回offline
     */
    OnlineStatus getClusterStatus(const std::string& cluster_name);
    /*
     * @brief   获得集群状态数组，内部申请，外部释放
     * @param   [in]    集群名
     * @param   [out]   集群状态数组
     * @return  0:成功； -1:失败
     */
    int32_t getClusterNodeStatus(const std::string& cluster_name, NodeStatusVec*& status_vec);
    /*
     * @brief   获取节点的状态信息
     * @param   [in]    节点的spec
     * @param   [in]    节点所在的集群
     * @param   [in/out]节点状态引用
     * @return  0:成功； -1:失败
     */
    int32_t getNodeStatus(const std::string& node_spec, const std::string& rstr_cluster_name, NodeStatus& node_status);
    /*
     * @brief   获取节点的MetaInfo信息
     * @param   [in]    节点的spec
     * @param   [in]    节点所在的集群
     * @param   [in/out]节点MetaInfo信息的指针
     * @return  0:成功； -1:失败
     */
    int32_t getNodeMetaInfo(const std::string& node_spec, const std::string& rstr_cluster_name,
                            NodeMetaInfo* meta_info);
};

} // namespace cm_basic

#endif // CM_SUB_CM_CENTRAL_SUB_H_
