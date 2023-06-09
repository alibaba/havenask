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
#include "aios/network/gig/multi_call/service/GrpcConnectionPool.h"
#include "aios/network/gig/multi_call/service/ConnectionFactory.h"

using namespace std;
using namespace autil::legacy;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GrpcConnectionPool);

GrpcConnectionPool::GrpcConnectionPool() {}

GrpcConnectionPool::~GrpcConnectionPool() {}

void GrpcConnectionPool::stop() {
    AUTIL_LOG(INFO, "GrpcConnectionPool stopped");
    if (_worker) {
        _worker->stop();
    }
    _worker.reset();
    ConnectionPool::stop();
}

bool GrpcConnectionPool::init(const ProtocolConfig &config) {
    AUTIL_LOG(INFO, "GrpcConnectionPool init, config [%s]",
              ToJsonString(config).c_str());
    _worker.reset(new GrpcClientWorker(config.threadNum));
    auto factory = new GrpcConnectionFactory(_worker, config.secureConfig);
    GrpcChannelInitParams params;
    params.keepAliveInterval = config.keepAliveInterval;
    params.keepAliveTimeout = config.keepAliveTimeout;
    factory->initChannelArgs(params);
    _connectionFactory.reset(factory);
    return true;
}

GrpcClientWorkerPtr &GrpcConnectionPool::getGrpcClientWorker() {
    return _worker;
}

} // namespace multi_call
