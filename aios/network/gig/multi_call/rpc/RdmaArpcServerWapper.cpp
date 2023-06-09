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
#include "multi_call/rpc/RdmaArpcServerWapper.h"
#include "aios/network/gig/multi_call/arpc/ServiceWrapper.h"

#ifndef AIOS_OPEN_SOURCE
#include "autil/NetUtil.h"
#include "aios/network/rdma/arpc/metric/KMonitorRdmaServerMetricReporter.h"
#include "aios/network/rdma/arpc/RdmaRPCServer.h"
#endif

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, RdmaArpcServerWapper);

RdmaArpcServerWapper::RdmaArpcServerWapper() {
}

RdmaArpcServerWapper::~RdmaArpcServerWapper() {
    stopRdmaArpcServer();
}

bool RdmaArpcServerWapper::startRdmaArpcServer(RdmaArpcServerDescription &desc) {
#ifndef AIOS_OPEN_SOURCE
    if (_rdmaArpcServer) {
        AUTIL_LOG(ERROR, "rdma arpc server already started");
        return false;
    }
    _rdmaArpcDesc = desc;

    arpc::RdmaRPCServerConfig rdmaServerConfig;
    rdmaServerConfig.arpcWorkerThreadNum = desc.arpcWorkerThreadNum;
    rdmaServerConfig.arpcWorkerQueueSize = desc.arpcWorkerQueueSize;
    rdmaServerConfig.rdmaIoThreadNum = desc.rdmaIoThreadNum;
    rdmaServerConfig.rdmaWorkerThreadNum = desc.rdmaWorkerThreadNum;
    _rdmaArpcServer.reset(new arpc::RdmaRPCServer(rdmaServerConfig));

    arpc::KMonitorRdmaMetricReporterConfig metricConfig;
    _rdmaArpcServer->setMetricReporter(
        std::make_unique<arpc::KMonitorRdmaServerMetricReporter>(metricConfig));

    if (!_rdmaArpcServer->Init()) {
        AUTIL_LOG(ERROR, "rdma arpc server init failed");
        return false;
    }

    AUTIL_LOG(INFO,
              "rdma arpc server start success, arpc worker thread: %d, queue "
              "size: %d, rdma worker thread %d io thread: %d",
              desc.arpcWorkerThreadNum, desc.arpcWorkerQueueSize,
              desc.rdmaIoThreadNum, desc.rdmaWorkerThreadNum);

    if (desc.ip.empty()) {
        desc.ip = autil::NetUtil::getBindIp();
    }
    std::string spec("tcp:");
    spec = spec + desc.ip + ":" + desc.port;
    if (!_rdmaArpcServer->Listen(spec)) {
        AUTIL_LOG(ERROR, "rdma arpc server listen on %s failed", spec.c_str());
        return false;
    }
    AUTIL_LOG(INFO, "rdma arpc server listen on %s success", spec.c_str());
    return true;
#else
    return false;
#endif
}

int32_t RdmaArpcServerWapper::getListenPort() {
#ifndef AIOS_OPEN_SOURCE
    if (_rdmaArpcServer) {
        return _rdmaArpcServer->GetListenPort();
    }
#endif
    return INVALID_PORT;
}

void RdmaArpcServerWapper::registerService(ServiceWrapper* serviceWrapper,
        const arpc::ThreadPoolDescriptor &threadPoolDescriptor)
{
    doRegisterService<arpc::ThreadPoolDescriptor>(serviceWrapper, threadPoolDescriptor);
}

void RdmaArpcServerWapper::registerService(ServiceWrapper* serviceWrapper,
                     const autil::ThreadPoolBasePtr &pool)
{
    doRegisterService<autil::ThreadPoolBasePtr>(serviceWrapper, pool);
}

template <typename ThreadPoolRep>
void RdmaArpcServerWapper::doRegisterService(ServiceWrapper* serviceWrapper,
        const ThreadPoolRep &poolRep)
{
#ifndef AIOS_OPEN_SOURCE
    if (_rdmaArpcServer) {
        _rdmaArpcServer->RegisterService(serviceWrapper, poolRep);
    }
#endif
}

void RdmaArpcServerWapper::stopRdmaArpcServer() {
#ifndef AIOS_OPEN_SOURCE
    if (_rdmaArpcServer) {
        _rdmaArpcServer->Close();
        _rdmaArpcServer.reset();
    }
    AUTIL_LOG(INFO, "rdma arpc server stoped");
#endif
}

}
