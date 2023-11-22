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
#ifndef __CM_SUBSCRIBER_IMP_H_
#define __CM_SUBSCRIBER_IMP_H_

#include <functional>

#include "aios/apps/facility/cm2/cm_sub/cm_sub_base.h"
#include "aios/network/arpc/arpc/ANetRPCChannel.h"
#include "aios/network/arpc/arpc/ANetRPCChannelManager.h"
#include "aios/network/arpc/arpc/ANetRPCController.h"
#include "aios/network/arpc/arpc/RPCInterface.h"
#include "autil/Lock.h"
#include "autil/Thread.h"

namespace cm_basic {
class SubscriberService;
class SubscriberService_Stub;
class SubReqMsg;
class SubRespMsg;
class CMCentralSub;
} // namespace cm_basic

namespace cm_sub {

class TopoClusterManager;
class IServerResolver;

class CMSubscriberImp : public CMSubscriberBase
{
public:
    CMSubscriberImp(SubscriberConfig* cfg_info, arpc::ANetRPCChannelManager* arpc, IServerResolver* server_resolver);
    virtual ~CMSubscriberImp();

public:
    virtual int32_t init(TopoClusterManager* topo_cluster, cm_basic::CMCentralSub* cm_central);
    virtual int32_t subscriber();
    virtual int32_t unsubscriber();
    virtual int32_t addSubCluster(std::string name);
    virtual int32_t removeSubCluster(std::string name);

private:
    // 连接server
    int32_t connect();
    int32_t delayConnect();
    // 与server通讯，更新内部信息
    int32_t update();
    void writeCacheProc();
    void doWriteCache();
    int32_t readCache();
    cm_basic::SubReqMsg getReqMsg();

private:
    //
    void updateReqClusterVersion();
    // 处理订阅的响应消息
    int32_t procSubRespMsg(cm_basic::SubRespMsg* resp_msg);
    static void* threadProc(void* argv);

private:
    arpc::ANetRPCChannelManager* _arpc; //
    IServerResolver* _serverResolver;
    bool _stopFlag;
    int32_t _retryMax;
    int32_t _expireTime;
    cm_basic::SubscriberService_Stub* _stub; //
    cm_basic::SubReqMsg* _reqMsg;
    autil::ThreadPtr _threadPtr; // 线程指针
    autil::ThreadPtr _writeCacheThread;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace cm_sub
#endif
