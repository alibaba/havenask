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
#ifndef __CM_SUBSCRIBER_BASE_H_
#define __CM_SUBSCRIBER_BASE_H_

#include <vector>

#include "aios/apps/facility/cm2/cm_sub/config/subscriber_config.h"
#include "aios/apps/facility/cm2/cm_sub/sub_resp_msg_process.h"

namespace cm_basic {
class CMCentralSub;
}

namespace cm_sub {

class TopoClusterManager;
class SubscriberConfig;

class CMSubscriberBase : public SubRespMsgProcess
{
protected:
    SubscriberConfig* _cfgInfo;

public:
    CMSubscriberBase(SubscriberConfig* cfg_info) : SubRespMsgProcess(cfg_info->_disableTopo), _cfgInfo(cfg_info) {}
    virtual ~CMSubscriberBase() {}
    virtual int32_t init(TopoClusterManager* topo_cluster, cm_basic::CMCentralSub* cm_central) = 0;
    virtual int32_t subscriber() { return 0; }
    virtual int32_t unsubscriber() { return 0; }
    virtual int32_t addSubCluster(std::string name) { return 0; };
    virtual int32_t removeSubCluster(std::string name) { return 0; };

protected:
    int32_t reinitLocal(const std::vector<cm_basic::CMCluster*>& clusters);
};

} // namespace cm_sub
#endif
