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
 *       Filename:  serialized_cluster.h
 *
 *    Description:  序列化集群的结构体，包括集群全量信息或者状态信息，多个订阅者复用
 *
 *        Version:  0.1
 *        Created:  2013-03-27 15:11:09
 *       Revision:  none
 *       Compiler:  glibc
 *
 *         Author:  tiechou (search engine group), tiechou@taobao.com
 *        Company:  www.taobao.com
 *
 * =====================================================================================
 */

#ifndef __SERIALIZED_CLUSTER_H_
#define __SERIALIZED_CLUSTER_H_

#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/cm_subscriber.pb.h"
#include "aios/apps/facility/cm2/cm_basic/common/common.h"

namespace cm_basic {

class SerializedCluster
{
public:
    ClusterUpdateInfo::MsgType msg_type;      ///< 消息类型
    int64_t version;                          ///< 集群最新的版本号
    char* cm_cluster[CompressType_ARRAYSIZE]; ///< 集群的结构化信息，只有集群版本更新的时候才更新
    uint32_t cluster_len[CompressType_ARRAYSIZE]; ///< 集群的结构化信息长度
    char* cluster_status[CompressType_ARRAYSIZE]; ///< 集群的状态信息，每隔一段时间更新
    uint32_t status_len[CompressType_ARRAYSIZE];  ///< 集群的状态信息长度
public:
    SerializedCluster() : msg_type(ClusterUpdateInfo::MT_UNKNOWN), version(0)
    {
        memset(cm_cluster, 0, sizeof(cm_cluster));
        memset(cluster_len, 0, sizeof(cluster_len));
        memset(cluster_status, 0, sizeof(cluster_status));
        memset(status_len, 0, sizeof(status_len));
    }
    ~SerializedCluster()
    {
        releaseCMCluster();
        releaseClusterStatus();
    }
    void releaseCMCluster()
    {
        for (int i = 0; i < CompressType_ARRAYSIZE; ++i) {
            if (cm_cluster[i]) {
                delete[] cm_cluster[i];
                cm_cluster[i] = NULL;
            }
            cluster_len[i] = 0;
        }
    }
    void releaseClusterStatus()
    {
        for (int i = 0; i < CompressType_ARRAYSIZE; ++i) {
            if (cluster_status[i]) {
                delete[] cluster_status[i];
                cluster_status[i] = NULL;
            }
            status_len[i] = 0;
        }
    }
};

} // namespace cm_basic
#endif
