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
 *       Filename:  config_manager.h
 *
 *    Description:  clustermap.xml 配置文件管理程序
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

#ifndef _CM_CONFIG_CONFIG_MANAGER_H_
#define _CM_CONFIG_CONFIG_MANAGER_H_

#include <map>
#include <vector>

#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/cm_common.pb.h"
#include "autil/Log.h"
#include "mxml.h"

namespace cm_basic {
/**
 * @class ClustermapConfig
 * @brief cluster对应的配置文件解析
 * @Change logs:
 */
class ClustermapConfig
{
public:
    ClustermapConfig();
    ~ClustermapConfig();

public:
    /**
     * @brief 对pszConfBuf字符串里面的内容进行解析，放置在CMCluster中
     * @param pszConfBuf,cluster对应的配置文件的内容
     * @param chk_type, if < 0 use config file value otherwise use this type
     * @param extra, load extra properties e.g: online status, node status
     * @return 0表示成功，-1表示失败
     */
    int parse(const char* p_conf_buf, int chk_type = -1, bool extra = false);

    int parseFile(const char* file, int chk_type = -1, bool extra = false);

    /**
     * @brief 得到parse解析后的CMCluster
     * @return 失败返回NULL，成功返回解析成功后的CMCluster
     */
    cm_basic::CMCluster* getCMCluster();
    std::vector<cm_basic::CMCluster*>& getAllCluster();

    /**
     * @brief 拆分p_protoport对应的数据，数据正确，放入到CMNode中
     * @param p_protoport Node对应的protoport
     * @param p_cmnode 该node对应的CMNode节点，如果p_protoport正确，会把数据放入到CMNode中
     * @return 0表示成功，-1表示失败
     */
    int32_t splitProtoPort(const char* p_protoport, cm_basic::CMNode* p_cmnode);
    void release();

    // cm_ctrl require this
    void parseMetaInfo(CMNode* node, const char* meta_str);

private:
    /*
     * key 为topo_info的table name, value为table下的每个part对应的node数量
     * 每个part下的node的数量大于0，并且相等
     */
    typedef std::map<std::string, std::vector<int>> TableName2Group;

    int32_t parseNumAttr(int32_t& rn_value, const char* p_value, bool negative = false);

    int32_t parseCheckType(const char* str, cm_basic::CMCluster::NodeCheckType& check_type);

    int32_t parseTopoType(const char* str, cm_basic::CMCluster::TopoType& topo_type);

    int32_t parseOnlineStatus(const char* str, OnlineStatus& status);

    int32_t parseNodeStatus(const char* str, NodeStatus& status);

    int parseNode(mxml_node_t* p_group_xmlnode, const char* p_cluster_name, int32_t n_group_id, int32_t& n_node_id,
                  TableName2Group& map_topo_info, bool extra);

    int parseGroup(mxml_node_t* p_cluster_node, const char* p_cluster_name, bool extra);

    cm_basic::CMCluster* parseCluster(mxml_node_t* p_cluster_xmlnode, int chk_type, bool extra);
    /**
     * @brief groupid是否正确，是否存在，是否是从1开始依次有序
     * @param rv_group_id cluster对应的group_id的集合
     * @return 0 表示成功，-1表示失败
     */
    int32_t checkGroupId(std::vector<int>& rv_group_id);
    /**
     * @brief 检查Node ip是否正确，格式，数据等
     * @param p_node_ip :node对应的ip
     * @return 0表示成功，-1表示失败，即错误
     */
    int32_t checkNodeIp(const char* p_node_ip);
    /**
     * @brief 对topoinfo解析，把数据加入到_map_topo_info
     * @param p_node_topoinfo，为node中topo_info的内容
     * @return 0表示成功，-1表示失败
     */
    int addTopoInfo(const char* p_node_topoinfo, TableName2Group& map_topo_info);
    /**
     * @brief 检查_map_topo_info的数据是否正确
     * @reutnr 数据正确返回0，否则返回-1
     */
    int checkTopoInfo(TableName2Group& map_topo_info);

private:
    ///< CMCluster数组，一个配置文件包含多个集群的情况
    std::vector<cm_basic::CMCluster*> _vecCMCluster;
    cm_basic::CMCluster* _p_cm_cluster;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace cm_basic
#endif
