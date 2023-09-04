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
#ifndef INCLUDE_CONFIG_OPTION_H_
#define INCLUDE_CONFIG_OPTION_H_

// Cluster
#define XML_NODE_CLUSTER "cluster"
#define XML_NODE_CLUSTER_NAME "name"

#define XML_NODE_BELONG "belong"

#define XML_NODE_CLUSTER_TOPOTYPE "topo_type"
#define CLUSTER_METAFORMAT_ONEMAPONE_OPTION "one_map_one"
#define CLUSTER_METAFORMAT_MAPTABLE_OPTION "cluster_map_table"

#define XML_NODE_CLUSTER_CPULIMIT "cpu_limit"
#define XML_NODE_CLUSTER_LOADONELIMIT "load_one_limit"
#define XML_NODE_CLUSTER_IOWAITLIMIT "iowait_limit"
#define XML_NODE_CLUSTER_LATENCYLIMIT "latency_limit"
#define XML_NODE_CLUSTER_QPSRATIOLIMIT "qps_ratio_limit"

#define XML_NODE_CLUSTER_CHECKFILENAME "check_file"
#define CLUSTER_CHECKFILENAME_DEFAULT_OPTION "status.html"

#define XML_NODE_CLUSTER_AUTOOFFLINE "auto_offline"

#define XML_NODE_CLUSTER_IDCPREFERRATIO_WATERMARK "idc_prefer_ratio_watermark"
#define XML_NODE_CLUSTER_IDCPREFERRATIO_WATERMARK_NEW "idc_prefer_ratio_watermark_new"
#define XML_NODE_CLUSTER_CLOSE_IDCPREFERRATIO_WATERMARK_NEW "close_idc_prefer_new"

#define XML_NODE_CLUSTER_PROTECT_WATERMARK "protect_ratio"

#define XML_NODE_CLUSTER_CHECKTYPE "check_type"
#define CLUSTER_CHECKTYPE_HEARTBEAT "heartbeat"
#define CLUSTER_CHECKTYPE_7LEVEL "7level_check"
#define CLUSTER_CHECKTYPE_4LEVEL "4level_check"
#define CLUSTER_CHECKTYPE_KEEP_ONLINE "keep_online"
#define CLUSTER_CHECKTYPE_KEEP_OFFLINE "keep_offline"
#define CLUSTER_CHECKTYPE_CASCADE "cascade"

// Group
#define XML_NODE_GROUP "group"
#define XML_NODE_GROUP_ID "id"
// Node
#define XML_NODE_NODE "node"
#define XML_NODE_NODE_IP "ip"
#define XML_NODE_NODE_PROTOPORT "proto_port"
#define XML_NODE_NODE_WEIGHT "weight"
#define XML_NODE_NODE_IDCTYPE "idc_type"
#define XML_NODE_NODE_TOPOINFO "topo_info"
#define XML_NODE_PROTOPORT_SPLIT '|'
#define XML_NODE_NODE_METAINFO "meta_info"

#define CLUSTERMAP_XML_HEADER "<?xml version=\"1.0\"\?>\n\n<clustermap>\n"
#define CLUSTERMAP_XML_HEADER_END "</clustermap>\n"

#define XML_NODE_ONLINESTATUS "online_status" // ONLINE/OFFLINE/etc
#define XML_NODE_NODE_STATUS "status"         // NORMAL/ABNORMAL/etc

#endif
