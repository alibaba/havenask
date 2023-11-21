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
#include "aios/apps/facility/cm2/cm_basic/config/config_manager.h"

#include "aios/apps/facility/cm2/cm_basic/basic_struct/cm_central.h"
#include "aios/apps/facility/cm2/cm_basic/common/common.h"
#include "aios/apps/facility/cm2/cm_basic/config/config_option.h"
#include "aios/apps/facility/cm2/cm_basic/util/file_util.h"
#include "aios/apps/facility/cm2/cm_basic/util/str_util.h"
#include "autil/StringUtil.h"

namespace cm_basic {

AUTIL_LOG_SETUP(cm_basic, ClustermapConfig);

void ClustermapConfig::release()
{
    for (uint32_t i = 0; i < _vecCMCluster.size(); ++i) {
        deletePtr(_vecCMCluster[i]);
    }
    _vecCMCluster.clear();
}

ClustermapConfig::ClustermapConfig() {}

ClustermapConfig::~ClustermapConfig() { release(); }

int32_t ClustermapConfig::parseNumAttr(int32_t& rn_value, const char* p_value, bool negative)
{
    if (p_value) {
        if (negative == false && atoi(p_value) <= 0) {
            return -1;
        }
        rn_value = atoi(p_value);
        return 0;
    }
    return -1;
}

int32_t ClustermapConfig::parseCheckType(const char* str, CMCluster::NodeCheckType& check_type)
{
    if (0 == strcasecmp(str, CLUSTER_CHECKTYPE_HEARTBEAT)) {
        check_type = CMCluster::NCT_HEARTBEAT;
    } else if (0 == strcasecmp(str, CLUSTER_CHECKTYPE_7LEVEL)) {
        check_type = CMCluster::NCT_7LEVEL_CHECK;
    } else if (0 == strcasecmp(str, CLUSTER_CHECKTYPE_4LEVEL)) {
        check_type = CMCluster::NCT_4LEVEL_CHECK;
    } else if (0 == strcasecmp(str, CLUSTER_CHECKTYPE_KEEP_ONLINE)) {
        check_type = CMCluster::NCT_KEEP_ONLINE;
    } else if (0 == strcasecmp(str, CLUSTER_CHECKTYPE_KEEP_OFFLINE)) {
        check_type = CMCluster::NCT_KEEP_OFFLINE;
    } else if (0 == strcasecmp(str, CLUSTER_CHECKTYPE_CASCADE)) {
        check_type = CMCluster::NCT_CASCADE;
    } else {
        return -1;
    }
    return 0;
}

int32_t ClustermapConfig::parseTopoType(const char* str, CMCluster::TopoType& topo_type)
{
    if (0 == strcasecmp(str, CLUSTER_METAFORMAT_ONEMAPONE_OPTION)) {
        topo_type = CMCluster::TT_ONE_MAP_ONE;
    } else if (0 == strcasecmp(str, CLUSTER_METAFORMAT_MAPTABLE_OPTION)) {
        topo_type = CMCluster::TT_CLUSTER_MAP_TABLE;
    } else {
        return -1;
    }
    return 0;
}

int32_t ClustermapConfig::parseOnlineStatus(const char* str, OnlineStatus& status)
{
    if (0 == strcasecmp(str, enum_online_status[OS_ONLINE])) {
        status = OS_ONLINE;
    } else if (0 == strcasecmp(str, enum_online_status[OS_OFFLINE])) {
        status = OS_OFFLINE;
    } // TODO: can't be OS_INITING
    else {
        return -1;
    }
    return 0;
}

int32_t ClustermapConfig::parseNodeStatus(const char* str, NodeStatus& status)
{
    for (int i = NS_NORMAL; i <= NS_UNINIT; ++i) {
        if (0 == strcasecmp(str, enum_node_status[i])) {
            status = (NodeStatus)i;
            return 0;
        }
    }
    return -1;
}

int ClustermapConfig::parseFile(const char* file, int chk_type, bool extra)
{
    std::string str = FileUtil::readFile(file);
    return parse(str.c_str(), chk_type, extra);
}

int ClustermapConfig::parse(const char* p_conf_buf, int chk_type, bool extra)
{
    release();
    mxml_node_t* p_xml_root = NULL;
    if (NULL == p_conf_buf) {
        AUTIL_LOG(WARN, "parse failed, conf_buf is NULL!");
        return -1;
    }
    //
    p_xml_root = mxmlLoadString(NULL, p_conf_buf, MXML_NO_CALLBACK);
    if (NULL == p_xml_root) {
        AUTIL_LOG(WARN, "parse failed, mxmlLoadString failed!");
        return -1;
    }

    mxml_node_t* p_cluster_xmlnode =
        mxmlFindElement(p_xml_root, p_xml_root, XML_NODE_CLUSTER, NULL, NULL, MXML_DESCEND);
    while (NULL != p_cluster_xmlnode) {
        CMCluster* cm_cluster = parseCluster(p_cluster_xmlnode, chk_type, extra);
        if (cm_cluster == NULL) {
            ///< 多个cluster, 有一个不成功就直接返回错误
            release();
            mxmlDelete(p_xml_root);
            return -1;
        }
        _vecCMCluster.push_back(cm_cluster);
        p_cluster_xmlnode = mxmlFindElement(p_cluster_xmlnode, p_xml_root, XML_NODE_CLUSTER, NULL, NULL, MXML_DESCEND);
    }
    if (_vecCMCluster.size() == 0) {
        // 没有cluster
        AUTIL_LOG(WARN, "cann't found %s lable!", XML_NODE_CLUSTER);
        mxmlDelete(p_xml_root);
        return -1;
    }
    mxmlDelete(p_xml_root);
    return 0;
}

CMCluster* ClustermapConfig::parseCluster(mxml_node_t* p_cluster_xmlnode, int chk_type, bool extra)
{
    do {
        _p_cm_cluster = new (std::nothrow) CMCluster();
        if (NULL == _p_cm_cluster) {
            AUTIL_LOG(WARN, "new CMCluster() failed!");
            break;
        }
        _p_cm_cluster->set_create_time(0);
        _p_cm_cluster->set_version(0);
        _p_cm_cluster->set_cluster_status(OS_ONLINE);
        if (chk_type < 0)
            _p_cm_cluster->set_check_type(CMCluster::NCT_HEARTBEAT);
        else
            _p_cm_cluster->set_check_type((CMCluster::NodeCheckType)chk_type);
        _p_cm_cluster->set_topo_type(CMCluster::TT_ONE_MAP_ONE);
        _p_cm_cluster->set_node_num(0);
        const char* p_cluster_name = mxmlElementGetAttr(p_cluster_xmlnode, XML_NODE_CLUSTER_NAME);
        if (NULL == p_cluster_name || '\0' == *p_cluster_name) {
            AUTIL_LOG(WARN, "cluster:cluster name is NULL!");
            break;
        }
        _p_cm_cluster->set_cluster_name(p_cluster_name);
        // online status
        const char* online_status_str = mxmlElementGetAttr(p_cluster_xmlnode, XML_NODE_ONLINESTATUS);
        if (extra && online_status_str) {
            OnlineStatus online_status;
            if (parseOnlineStatus(online_status_str, online_status) == 0) {
                _p_cm_cluster->set_cluster_status(online_status);
            }
        }
        const char* belong_str = mxmlElementGetAttr(p_cluster_xmlnode, XML_NODE_BELONG);
        if (belong_str) {
            std::vector<std::string> belongs = autil::StringUtil::split(belong_str, ",");
            for (size_t i = 0; i < belongs.size(); ++i) {
                _p_cm_cluster->add_belong_vec(belongs[i]);
            }
        }

        const char* p_check_type = mxmlElementGetAttr(p_cluster_xmlnode, XML_NODE_CLUSTER_CHECKTYPE);
        if (chk_type < 0 && p_check_type != NULL) {
            CMCluster::NodeCheckType check_type;
            if (parseCheckType(p_check_type, check_type) == -1) {
                AUTIL_LOG(WARN, "parseCheckType(%s, %s) failed !!", XML_NODE_CLUSTER_CHECKTYPE, p_check_type);
                break;
            }
            _p_cm_cluster->set_check_type(check_type);
        }
        bool b_auto_offline = false;
        const char* p_limit_attr = mxmlElementGetAttr(p_cluster_xmlnode, XML_NODE_CLUSTER_CPULIMIT);
        if (p_limit_attr != NULL) {
            NodeLimit* p_node_limit = _p_cm_cluster->mutable_node_limits();
            int32_t n_limit_attr = 0;
            if (parseNumAttr(n_limit_attr, p_limit_attr) < 0) {
                AUTIL_LOG(WARN, "cluster:%s=%s is wrong!", XML_NODE_CLUSTER_CPULIMIT, p_limit_attr);
                break;
            }
            p_node_limit->set_cpu_busy_limit(n_limit_attr);
            b_auto_offline = true;
        }
        p_limit_attr = mxmlElementGetAttr(p_cluster_xmlnode, XML_NODE_CLUSTER_LOADONELIMIT);
        if (p_limit_attr != NULL) {
            NodeLimit* p_node_limit = _p_cm_cluster->mutable_node_limits();
            int32_t n_limit_attr = 0;
            if (parseNumAttr(n_limit_attr, p_limit_attr) < 0) {
                AUTIL_LOG(WARN, "cluster:%s=%s is wrong!", XML_NODE_CLUSTER_LOADONELIMIT, p_limit_attr);
                break;
            }
            p_node_limit->set_load_one_limit(n_limit_attr);
            b_auto_offline = true;
        }
        p_limit_attr = mxmlElementGetAttr(p_cluster_xmlnode, XML_NODE_CLUSTER_IOWAITLIMIT);
        if (p_limit_attr != NULL) {
            NodeLimit* p_node_limit = _p_cm_cluster->mutable_node_limits();
            int32_t n_limit_attr = 0;
            if (parseNumAttr(n_limit_attr, p_limit_attr) < 0) {
                AUTIL_LOG(WARN, "cluster:%s=%s is wrong!", XML_NODE_CLUSTER_IOWAITLIMIT, p_limit_attr);
                break;
            }
            p_node_limit->set_iowait_limit(n_limit_attr);
            b_auto_offline = true;
        }
        p_limit_attr = mxmlElementGetAttr(p_cluster_xmlnode, XML_NODE_CLUSTER_LATENCYLIMIT);
        if (p_limit_attr != NULL) {
            NodeLimit* p_node_limit = _p_cm_cluster->mutable_node_limits();
            int32_t n_limit_attr = 0;
            if (parseNumAttr(n_limit_attr, p_limit_attr) < 0) {
                AUTIL_LOG(WARN, "cluster:%s=%s is wrong!", XML_NODE_CLUSTER_LATENCYLIMIT, p_limit_attr);
                break;
            }
            p_node_limit->set_latency_limit(n_limit_attr);
            b_auto_offline = true;
        }
        p_limit_attr = mxmlElementGetAttr(p_cluster_xmlnode, XML_NODE_CLUSTER_QPSRATIOLIMIT);
        if (p_limit_attr != NULL) {
            NodeLimit* p_node_limit = _p_cm_cluster->mutable_node_limits();
            int32_t n_limit_attr = 0;
            if (parseNumAttr(n_limit_attr, p_limit_attr) < 0) {
                AUTIL_LOG(WARN, "cluster:%s=%s is wrong!", XML_NODE_CLUSTER_QPSRATIOLIMIT, p_limit_attr);
                break;
            }
            p_node_limit->set_qps_limit(n_limit_attr);
            b_auto_offline = true;
        }

        const char* p_auto_offline = mxmlElementGetAttr(p_cluster_xmlnode, XML_NODE_CLUSTER_AUTOOFFLINE);
        if (b_auto_offline && p_auto_offline != NULL) {
            if (0 == strcasecmp("yes", p_auto_offline)) {
                _p_cm_cluster->set_auto_offline(true);
            }
        }

        const char* p_idc_prefer_ratio =
            mxmlElementGetAttr(p_cluster_xmlnode, XML_NODE_CLUSTER_IDCPREFERRATIO_WATERMARK);
        if (p_idc_prefer_ratio != NULL) {
            int32_t n_idc_prefer_ratio = 0;
            if (parseNumAttr(n_idc_prefer_ratio, p_idc_prefer_ratio) < 0) {
                AUTIL_LOG(WARN, "cluster:%s=%s is wrong!", XML_NODE_CLUSTER_IDCPREFERRATIO_WATERMARK,
                          p_idc_prefer_ratio);
                break;
            }
            _p_cm_cluster->set_idc_prefer_ratio(n_idc_prefer_ratio);
        }

        const char* p_protect_ratio = mxmlElementGetAttr(p_cluster_xmlnode, XML_NODE_CLUSTER_PROTECT_WATERMARK);
        if (p_protect_ratio != NULL) {
            int protect_ratio = 0;
            if (parseNumAttr(protect_ratio, p_protect_ratio, true) < 0 || protect_ratio < -2 || protect_ratio > 100) {
                AUTIL_LOG(WARN, "cluster:%s=%s is wrong!", XML_NODE_CLUSTER_PROTECT_WATERMARK, p_protect_ratio);
                break;
            }
            _p_cm_cluster->set_protect_ratio(protect_ratio);
        }

        const char* p_idc_prefer_ratio_new =
            mxmlElementGetAttr(p_cluster_xmlnode, XML_NODE_CLUSTER_IDCPREFERRATIO_WATERMARK_NEW);
        if (p_idc_prefer_ratio_new != NULL) {
            int32_t n_idc_prefer_ratio_new = 0;
            if (parseNumAttr(n_idc_prefer_ratio_new, p_idc_prefer_ratio_new) < 0) {
                AUTIL_LOG(WARN, "cluster:%s=%s is wrong!", XML_NODE_CLUSTER_IDCPREFERRATIO_WATERMARK_NEW,
                          p_idc_prefer_ratio_new);
                break;
            }
            if (n_idc_prefer_ratio_new > 100) {
                n_idc_prefer_ratio_new = 100;
            } else if (n_idc_prefer_ratio_new < 0) {
                n_idc_prefer_ratio_new = 0;
            }
            _p_cm_cluster->set_idc_prefer_ratio_new(n_idc_prefer_ratio_new);
        }

        const char* p_close_idc_prefer_new =
            mxmlElementGetAttr(p_cluster_xmlnode, XML_NODE_CLUSTER_CLOSE_IDCPREFERRATIO_WATERMARK_NEW);
        if (p_close_idc_prefer_new != NULL) {
            if (0 == strcasecmp("yes", p_close_idc_prefer_new)) {
                _p_cm_cluster->set_close_idc_prefer_new(true);
            } else {
                _p_cm_cluster->set_close_idc_prefer_new(false);
            }
        }

        p_limit_attr = mxmlElementGetAttr(p_cluster_xmlnode, XML_NODE_CLUSTER_TOPOTYPE);
        if (p_limit_attr != NULL) {
            CMCluster::TopoType topo_type;
            if (parseTopoType(p_limit_attr, topo_type) == -1) {
                AUTIL_LOG(WARN, "parseTopoType(%s, %s) failed !!", XML_NODE_CLUSTER_TOPOTYPE, p_limit_attr);
                break;
            }
            _p_cm_cluster->set_topo_type(topo_type);
        }
        // 检查check_filename，如果是7层健康健康，并且没有配置checkfile，则用默认filename
        const char* p_check_filename = mxmlElementGetAttr(p_cluster_xmlnode, XML_NODE_CLUSTER_CHECKFILENAME);
        if (p_check_filename != NULL) {
            _p_cm_cluster->set_check_filename(p_check_filename);
        } else if (CMCluster::NCT_7LEVEL_CHECK == _p_cm_cluster->check_type()) {
            _p_cm_cluster->set_check_filename(CLUSTER_CHECKFILENAME_DEFAULT_OPTION);
        }

        int32_t n_node_num = 0;
        if ((n_node_num = parseGroup(p_cluster_xmlnode, p_cluster_name, extra)) < 0) {
            break;
        }
        _p_cm_cluster->set_node_num(n_node_num);
        return _p_cm_cluster;
    } while (0);
    deletePtr(_p_cm_cluster);
    return NULL;
}

int ClustermapConfig::checkGroupId(std::vector<int>& rv_group_id)
{
    if (rv_group_id.size() <= 0) {
        return 0;
    }
    // 要判断从0开始
    if (rv_group_id[0] != 0) {
        return -1;
    }
    // 是否要判断相邻
    for (int i = 1; i < (int)(rv_group_id.size()); ++i) {
        if ((rv_group_id[i] - rv_group_id[i - 1]) != 1) {
            return -1;
        }
    }
    return 0;
}

int ClustermapConfig::parseGroup(mxml_node_t* p_cluster_node, const char* p_cluster_name, bool extra)
{
    int32_t n_node_id = 0;
    std::vector<int> v_group_id;
    TableName2Group map_topo_info;
    for (mxml_node_t* p_group_xmlnode =
             mxmlFindElement(p_cluster_node, p_cluster_node, XML_NODE_GROUP, NULL, NULL, MXML_DESCEND);
         p_group_xmlnode;
         p_group_xmlnode = mxmlFindElement(p_group_xmlnode, p_cluster_node, XML_NODE_GROUP, NULL, NULL, MXML_DESCEND)) {
        const char* p_group_id_attr = mxmlElementGetAttr(p_group_xmlnode, XML_NODE_GROUP_ID);
        int32_t n_group_id_attr = 0;
        if (parseNumAttr(n_group_id_attr, p_group_id_attr, true) < 0) {
            AUTIL_LOG(WARN, "group:%s=%s is wrong!", XML_NODE_GROUP_ID, p_group_id_attr);
            return -1;
        }
        if (parseNode(p_group_xmlnode, p_cluster_name, n_group_id_attr, n_node_id, map_topo_info, extra) < 0) {
            return -1;
        }
        v_group_id.push_back(n_group_id_attr);
    }
    if (checkGroupId(v_group_id) < 0) {
        AUTIL_LOG(WARN, "groupid is wrong!");
        return -1;
    }
    // 要求每一列机器数相等
    if (checkTopoInfo(map_topo_info) < 0) {
        AUTIL_LOG(WARN, "topoinfo is wrong!");
        return -1;
    }
    return n_node_id;
}

int ClustermapConfig::checkNodeIp(const char* p_node_ip)
{
    std::vector<std::string> v_str_ip_part;
    splitStr(p_node_ip, '.', v_str_ip_part);
    if (v_str_ip_part.size() != 4) {
        return -1;
    }
    for (int i = 0; i < 4; ++i) {
        int n_num = atoi(v_str_ip_part[i].c_str());
        if (n_num < 0 || n_num > 255) {
            return -1;
        }
    }
    return 0;
}

int ClustermapConfig::parseNode(mxml_node_t* p_group_xmlnode, const char* p_cluster_name, int32_t n_group_id,
                                int32_t& n_node_id, TableName2Group& map_topo_info, bool extra)
{
    CMGroup* p_cm_group = _p_cm_cluster->add_group_vec();
    int n_node_idx = 0;
    for (mxml_node_t* p_node_xmlnode =
             mxmlFindElement(p_group_xmlnode, p_group_xmlnode, XML_NODE_NODE, NULL, NULL, MXML_DESCEND);
         p_node_xmlnode;
         p_node_xmlnode = mxmlFindElement(p_node_xmlnode, p_group_xmlnode, XML_NODE_NODE, NULL, NULL, MXML_DESCEND)) {
        CMNode* p_cm_node = p_cm_group->add_node_vec();
        // default value
        p_cm_node->set_group_id(-1);
        p_cm_node->set_online_status(OS_ONLINE);
        if (_p_cm_cluster->check_type() == CMCluster::NCT_KEEP_ONLINE) {
            p_cm_node->set_cur_status(NS_NORMAL);
        } else {
            p_cm_node->set_cur_status(NS_ABNORMAL);
        }
        p_cm_node->set_prev_status(NS_UNINIT);
        p_cm_node->set_valid_time(0);
        p_cm_node->set_cluster_name(p_cluster_name);
        p_cm_node->set_heartbeat_time(0);
        p_cm_node->set_start_time(0);
        const char* p_node_attr = mxmlElementGetAttr(p_node_xmlnode, XML_NODE_NODE_IP);
        if (NULL == p_node_attr) {
            AUTIL_LOG(WARN, "node:%s is NULL!", XML_NODE_NODE_IP);
            return -1;
        }
        if (checkNodeIp(p_node_attr) < 0) {
            AUTIL_LOG(WARN, "node:%s=%s is wrong!", XML_NODE_NODE_IP, p_node_attr);
            return -1;
        }

        // add by tiechou for trim space from ip(front and end)
        char* ip = trim(p_node_attr);
        p_cm_node->set_node_ip(ip);
        delete[] ip;

        if (extra) {
            OnlineStatus online_status;
            NodeStatus node_status;
            p_node_attr = mxmlElementGetAttr(p_node_xmlnode, XML_NODE_ONLINESTATUS);
            if (p_node_attr && parseOnlineStatus(p_node_attr, online_status) == 0) {
                p_cm_node->set_online_status(online_status);
            }
            p_node_attr = mxmlElementGetAttr(p_node_xmlnode, XML_NODE_NODE_STATUS);
            if (p_node_attr && parseNodeStatus(p_node_attr, node_status) == 0) {
                p_cm_node->set_cur_status(node_status);
            }
        }

        p_node_attr = mxmlElementGetAttr(p_node_xmlnode, XML_NODE_NODE_PROTOPORT);
        if (NULL == p_node_attr) {
            AUTIL_LOG(WARN, "node:%s is NULL!", XML_NODE_NODE_PROTOPORT);
            return -1;
        }
        if (splitProtoPort(p_node_attr, p_cm_node) < 0) {
            AUTIL_LOG(WARN, "node:%s=%s is wrong!", XML_NODE_NODE_PROTOPORT, p_node_attr);
            return -1;
        }
        p_node_attr = mxmlElementGetAttr(p_node_xmlnode, XML_NODE_NODE_WEIGHT);
        if (p_node_attr != NULL) {
            int32_t n_node_weight = 0;
            if (parseNumAttr(n_node_weight, p_node_attr) < 0) {
                AUTIL_LOG(WARN, "node:%s=%s is wrong!", XML_NODE_NODE_WEIGHT, p_node_attr);
                return -1;
            }
            NodeLBInfo* p_node_lbinfo = p_cm_node->mutable_lb_info();
            p_node_lbinfo->set_weight(n_node_weight);
        }

        // 解析idc_type字段
        p_node_attr = mxmlElementGetAttr(p_node_xmlnode, XML_NODE_NODE_IDCTYPE);
        if (p_node_attr != NULL) {
            int32_t n_idc_type = atoi(p_node_attr);
            if (n_idc_type < (int)IDC_ANY || n_idc_type >= (int)IDC_END) {
                AUTIL_LOG(WARN, "node:%s=%s is wrong!", XML_NODE_NODE_IDCTYPE, p_node_attr);
                return -1;
            }
            p_cm_node->set_idc_type((IDCType)(n_idc_type));
        }

        p_node_attr = mxmlElementGetAttr(p_node_xmlnode, XML_NODE_NODE_METAINFO);
        if (p_node_attr != NULL) {
            parseMetaInfo(p_cm_node, p_node_attr);
        }

        // 解析topo_info，如果是TT_CLUSTER_MAP_TABLE的情况
        if (CMCluster::TT_CLUSTER_MAP_TABLE == _p_cm_cluster->topo_type()) {
            p_node_attr = mxmlElementGetAttr(p_node_xmlnode, XML_NODE_NODE_TOPOINFO);
            if (NULL == p_node_attr) {
                AUTIL_LOG(WARN, "node:topo_type=TT_CLUSTER_MAP_TABLE,but %s is NULL!", XML_NODE_NODE_TOPOINFO);
                return -1;
            }
            if (addTopoInfo(p_node_attr, map_topo_info) < 0) {
                AUTIL_LOG(WARN, "node:topo_type=TT_CLUSTER_MAP_TABLE, %s=%s is wrong!", XML_NODE_NODE_TOPOINFO,
                          p_node_attr);
                return -1;
            }
            p_cm_node->set_topo_info(p_node_attr);
        }
        p_cm_node->set_group_id(n_group_id);
        ++n_node_idx;
    }
    if (n_node_idx != 0) {
        n_node_id += n_node_idx;
        return 0;
    }
    return 0;
}

void ClustermapConfig::parseMetaInfo(CMNode* node, const char* meta_str)
{
    std::vector<std::string> metainfos;
    splitStr(meta_str, '|', metainfos);
    for (size_t i = 0; i < metainfos.size(); ++i) {
        std::vector<std::string> pair;
        splitStr(metainfos[i].c_str(), ':', pair);
        if (pair.size() == 0)
            continue;
        NodeMetaInfo* metainfo = node->mutable_meta_info();
        MetaKV* kv = metainfo->add_kv_array();
        kv->set_meta_key(pair[0]);
        if (pair.size() > 1)
            kv->set_meta_value(pair[1]);
    }
}

int ClustermapConfig::addTopoInfo(const char* p_node_topoinfo, TableName2Group& map_topo_info)
{
    // p_node_topoinfo 判空操作需要在外面判断
    std::vector<std::string> v_topo_info;
    splitStr(p_node_topoinfo, '|', v_topo_info);
    int32_t n_topoinfo_num = v_topo_info.size();
    if (n_topoinfo_num <= 0) {
        return -1;
    }
    for (int32_t i = 0; i < n_topoinfo_num; ++i) {
        std::vector<std::string> v_topoinfo_attr;
        splitStr(v_topo_info[i].c_str(), ':', v_topoinfo_attr);
        // 格式是否正确
        if (v_topoinfo_attr.size() < 3) {
            return -1;
        }
        int32_t n_group_num = atoi(v_topoinfo_attr[1].c_str());
        if (n_group_num <= 0) {
            return -1;
        }
        int32_t n_group_id = atoi(v_topoinfo_attr[2].c_str());
        if (n_group_id < 0 || n_group_id >= n_group_num) {
            return -1;
        }
        std::vector<int> v_group_info(n_group_num, 0);
        v_group_info[n_group_id]++;
        std::pair<TableName2Group::iterator, bool> pair_res =
            map_topo_info.insert(TableName2Group::value_type(v_topoinfo_attr[0], v_group_info));
        if (!pair_res.second) {
            // 判断当前的group的数量和以前记录的是否相同
            if ((int)(pair_res.first->second).size() != n_group_num) {
                return -1;
            }
            (pair_res.first->second)[n_group_id]++;
        }
    }
    return 0;
}

int ClustermapConfig::checkTopoInfo(TableName2Group& map_topo_info)
{
    for (TableName2Group::iterator itea = map_topo_info.begin(); itea != map_topo_info.end(); ++itea) {
        std::vector<int> rv_group_id = itea->second;
        int32_t n_group_num = rv_group_id.size();
        if (n_group_num < 0) {
            return -1;
        } else if (1 == n_group_num) {
            // group对应的node数量是否要>1，==1是不是也是错误
            if (rv_group_id[0] <= 0) {
                return -1;
            }
        } else {
            int n_num = rv_group_id[0];
            if (n_num <= 0) {
                return -1;
            }
            for (int i = 1; i < (int)rv_group_id.size(); ++i) {
                if (rv_group_id[i] != n_num) {
                    return -1;
                }
            }
        }
    }
    return 0;
}

int32_t ClustermapConfig::splitProtoPort(const char* p_protoport, CMNode* p_cmnode)
{
    std::vector<std::string> v_protoport;
    splitStr(p_protoport, '|', v_protoport);
    int32_t n_protoport_num = v_protoport.size();
    if (n_protoport_num <= 0) {
        return -1;
    }
    for (int32_t i = 0; i < n_protoport_num; ++i) {
        std::vector<std::string> v_protoport_attr;
        splitStr(v_protoport[i].c_str(), ':', v_protoport_attr);
        // proto_port的格式可能不对
        if (v_protoport_attr.size() != 2) {
            return -1;
        }
        ProtocolPort* p_node_protoport = p_cmnode->add_proto_port();
        // 协议在没在正确的范围值内
        if (0 == v_protoport_attr[0].compare("tcp")) {
            p_node_protoport->set_protocol(PT_TCP);
        } else if (0 == v_protoport_attr[0].compare("udp")) {
            p_node_protoport->set_protocol(PT_UDP);
        } else if (0 == v_protoport_attr[0].compare("http")) {
            p_node_protoport->set_protocol(PT_HTTP);
        } else {
            return -1;
        }
        // port是否存在，或正确，需要>0
        if (v_protoport_attr[1].c_str() != NULL && *(v_protoport_attr[1].c_str()) != '\0') {
            int32_t n_port = 0;
            if (parseNumAttr(n_port, v_protoport_attr[1].c_str()) < 0) {
                return -1;
            }
            p_node_protoport->set_port(n_port);
        } else {
            return -1;
        }
    }
    return 0;
}

CMCluster* ClustermapConfig::getCMCluster()
{
    ///< 兼容老的配置，返回第一个集群
    if (_vecCMCluster.size() > 0) {
        return _vecCMCluster[0];
    }
    return NULL;
}
std::vector<CMCluster*>& ClustermapConfig::getAllCluster() { return _vecCMCluster; }

} // namespace cm_basic
