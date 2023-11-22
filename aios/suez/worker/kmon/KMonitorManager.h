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
#pragma once

#include <memory>
#include <sstream>

#include "autil/Lock.h"
#include "suez/sdk/KMonitorMetaInfo.h"

namespace kmonitor {
class MetricsSystem;
} // namespace kmonitor

namespace suez {

class ManuallySink;
class KMonitorDebugService;
class RpcServer;
struct EnvParam;

class KMonitorManager {
public:
    KMonitorManager();
    ~KMonitorManager();
    KMonitorManager(const KMonitorManager &) = delete;
    const KMonitorManager &operator=(const KMonitorManager &) = delete;

public:
    bool init(const EnvParam &param);
    bool postInit(const EnvParam &param, RpcServer *server);

public:
    const KMonitorMetaInfo &getMetaInfo() const;
    std::string manuallySnapshot();

private:
    bool initMetricsFactory(const EnvParam &param);
    bool registerRpcService(RpcServer *server);
    kmonitor::MetricsSystem *getMetricsSystem() const;

private:
    KMonitorMetaInfo _metaInfo;
    std::unique_ptr<KMonitorDebugService> _rpcService;
    autil::ThreadMutex _manuallySnapshotLock;
    std::shared_ptr<ManuallySink> _sink;
};

} // namespace suez
