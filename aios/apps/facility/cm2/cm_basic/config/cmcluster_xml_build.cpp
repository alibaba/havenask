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
#include "aios/apps/facility/cm2/cm_basic/config/cmcluster_xml_build.h"

#include <assert.h>
#include <mxml.h>

#include "aios/apps/facility/cm2/cm_basic/basic_struct/cm_central.h"
#include "aios/apps/facility/cm2/cm_basic/config/config_option.h"
#include "aios/apps/facility/cm2/cm_basic/util/str_util.h"

namespace cm_basic {

const char* mxml_save_cb(mxml_node_t* node, int iWhere)
{
    const char* name = node->value.element.name;
    if (0 == strcmp(name, XML_NODE_CLUSTER)) {
        if (iWhere == MXML_WS_BEFORE_OPEN) {
            return ("    ");
        }
        if (iWhere == MXML_WS_BEFORE_CLOSE) {
            return ("\n    ");
        }
    }
    if (0 == strcmp(name, XML_NODE_GROUP)) {
        if (iWhere == MXML_WS_BEFORE_OPEN || iWhere == MXML_WS_BEFORE_CLOSE) {
            return ("\n        ");
        }
    }
    if (0 == strcmp(name, XML_NODE_NODE)) {
        if (iWhere == MXML_WS_BEFORE_OPEN || iWhere == MXML_WS_BEFORE_CLOSE) {
            return ("\n            ");
        }
    }
    return NULL;
}

// xx:xx|xx|xxx:xxx
static std::string makeMetaInfoStr(const CMNode* node)
{
    std::string info;
    assert(node->has_meta_info());
    const NodeMetaInfo& node_meta_info = node->meta_info();
    for (int32_t i = 0; i < node_meta_info.kv_array_size(); ++i) {
        if (info.length() > 0)
            info += "|";
        info += node_meta_info.kv_array(i).meta_key();
        if (node_meta_info.kv_array(i).has_meta_value()) {
            info += (":" + node_meta_info.kv_array(i).meta_value());
        }
    }
    return info;
}

char* CMClusterXMLBuild::buildXmlCMCluster(cm_basic::CMCluster* pCMCluster, bool extra)
{
    if (NULL == pCMCluster) {
        return NULL;
    }
    mxml_node_t* pRoot = mxmlNewXML("1.0.0");
    if (NULL == pRoot) {
        return NULL;
    }
    mxmlSetElement(pRoot, XML_NODE_CLUSTER);
    mxmlElementSetAttr(pRoot, XML_NODE_CLUSTER_NAME, pCMCluster->cluster_name().c_str());
    mxmlElementSetAttr(pRoot, XML_NODE_CLUSTER_TOPOTYPE, cm_basic::enum_topo_type[pCMCluster->topo_type()]);
    mxmlElementSetAttr(pRoot, XML_NODE_CLUSTER_CHECKTYPE, cm_basic::enum_check_type[pCMCluster->check_type()]);
    if (pCMCluster->belong_vec_size() > 0) {
        mxmlElementSetAttrf(pRoot, XML_NODE_BELONG, "%s", joinStr(pCMCluster->belong_vec()).c_str());
    }
    if (extra) {
        mxmlElementSetAttr(pRoot, XML_NODE_ONLINESTATUS, cm_basic::enum_online_status[pCMCluster->cluster_status()]);
    }
    if (pCMCluster->has_check_filename()) {
        mxmlElementSetAttr(pRoot, XML_NODE_CLUSTER_CHECKFILENAME, pCMCluster->check_filename().c_str());
    }
    if (pCMCluster->has_idc_prefer_ratio()) {
        mxmlElementSetAttrf(pRoot, XML_NODE_CLUSTER_IDCPREFERRATIO_WATERMARK, "%d", pCMCluster->idc_prefer_ratio());
    }
    if (pCMCluster->has_protect_ratio()) {
        mxmlElementSetAttrf(pRoot, XML_NODE_CLUSTER_PROTECT_WATERMARK, "%d", pCMCluster->protect_ratio());
    }
    if (pCMCluster->has_idc_prefer_ratio_new()) {
        mxmlElementSetAttrf(pRoot, XML_NODE_CLUSTER_IDCPREFERRATIO_WATERMARK_NEW, "%d",
                            pCMCluster->idc_prefer_ratio_new());
    }
    if (pCMCluster->has_close_idc_prefer_new()) {
        mxmlElementSetAttrf(pRoot, XML_NODE_CLUSTER_CLOSE_IDCPREFERRATIO_WATERMARK_NEW, "%s",
                            (pCMCluster->close_idc_prefer_new() ? "yes" : "no"));
    }
    if (pCMCluster->has_node_limits()) {
        const cm_basic::NodeLimit& rNodeLimit = pCMCluster->node_limits();
        if (rNodeLimit.has_cpu_busy_limit()) {
            mxmlElementSetAttrf(pRoot, XML_NODE_CLUSTER_CPULIMIT, "%d", rNodeLimit.cpu_busy_limit());
        }
        if (rNodeLimit.has_iowait_limit()) {
            mxmlElementSetAttrf(pRoot, XML_NODE_CLUSTER_IOWAITLIMIT, "%d", rNodeLimit.iowait_limit());
        }
        if (rNodeLimit.has_latency_limit()) {
            mxmlElementSetAttrf(pRoot, XML_NODE_CLUSTER_LATENCYLIMIT, "%d", rNodeLimit.latency_limit());
        }
        if (rNodeLimit.has_load_one_limit()) {
            mxmlElementSetAttrf(pRoot, XML_NODE_CLUSTER_LOADONELIMIT, "%d", rNodeLimit.load_one_limit());
        }
        if (rNodeLimit.has_qps_limit()) {
            mxmlElementSetAttrf(pRoot, XML_NODE_CLUSTER_QPSRATIOLIMIT, "%d", rNodeLimit.qps_limit());
        }
    }
    int iGroupNum = pCMCluster->group_vec_size();
    for (int iIdx = 0; iIdx < iGroupNum; ++iIdx) {
        char szBuf[256];
        mxml_node_t* pGroupChild = mxmlNewElement(pRoot, XML_NODE_GROUP);
        mxmlElementSetAttrf(pGroupChild, XML_NODE_GROUP_ID, "%d", iIdx);
        const cm_basic::CMGroup& rGroup = pCMCluster->group_vec(iIdx);
        int iNodeNum = rGroup.node_vec_size();
        for (int jIdx = 0; jIdx < iNodeNum; ++jIdx) {
            mxml_node_t* pNodeChild = mxmlNewElement(pGroupChild, XML_NODE_NODE);
            const cm_basic::CMNode& rNode = rGroup.node_vec(jIdx);
            mxmlElementSetAttr(pNodeChild, XML_NODE_NODE_IP, rNode.node_ip().c_str());
            int n_ret = 0;
            for (int iProtocIdx = 0; iProtocIdx < rNode.proto_port_size(); ++iProtocIdx) {
                const cm_basic::ProtocolPort& rProtocolPort = rNode.proto_port(iProtocIdx);
                n_ret += sprintf(szBuf + n_ret, "|%s:%d", cm_basic::enum_protocol_type[rProtocolPort.protocol()],
                                 rProtocolPort.port());
            }
            if (rNode.proto_port_size() > 0) {
                mxmlElementSetAttr(pNodeChild, XML_NODE_NODE_PROTOPORT, szBuf + 1);
            }
            if (rNode.has_lb_info()) {
                if (rNode.lb_info().has_weight()) {
                    mxmlElementSetAttrf(pNodeChild, XML_NODE_NODE_WEIGHT, "%d", rNode.lb_info().weight());
                }
            }
            if (rNode.has_topo_info()) {
                mxmlElementSetAttr(pNodeChild, XML_NODE_NODE_TOPOINFO, rNode.topo_info().c_str());
            }
            if (rNode.has_idc_type()) {
                mxmlElementSetAttrf(pNodeChild, XML_NODE_NODE_IDCTYPE, "%d", (int)rNode.idc_type());
            }
            if (rNode.has_meta_info()) {
                mxmlElementSetAttr(pNodeChild, XML_NODE_NODE_METAINFO, makeMetaInfoStr(&rNode).c_str());
            }
            if (extra) {
                mxmlElementSetAttr(pNodeChild, XML_NODE_NODE_STATUS, enum_node_status[rNode.cur_status()]);
                mxmlElementSetAttr(pNodeChild, XML_NODE_ONLINESTATUS, enum_online_status[rNode.online_status()]);
            }
        }
    }
    char* p_config_buff = mxmlSaveAllocString(pRoot, mxml_save_cb);
    mxmlDelete(pRoot);
    return p_config_buff;
}

} // namespace cm_basic
