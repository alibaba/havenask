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
#ifndef ISEARCH_BS_ADMINHEALTHCHECKER_H
#define ISEARCH_BS_ADMINHEALTHCHECKER_H

#include "autil/Lock.h"
#include "autil/LoopThread.h"
#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "kmonitor_adapter/Monitor.h"

namespace build_service { namespace admin {

class ServiceKeeper;
class GenerationKeeper;
BS_TYPEDEF_PTR(GenerationKeeper);

class AdminHealthChecker
{
public:
    AdminHealthChecker(kmonitor_adapter::Monitor* globalMonitor = nullptr);
    virtual ~AdminHealthChecker();

private:
    AdminHealthChecker(const AdminHealthChecker&);
    AdminHealthChecker& operator=(const AdminHealthChecker&);

public:
    bool init(ServiceKeeper* keeper, int64_t checkInterval = 10 * 1000 * 1000);
    bool addGenerationKeeper(const proto::BuildId buildId, const GenerationKeeperPtr& generationKeeper);

    void reportScheduleFreshness(int64_t value) const;

protected:
    void checkHealth();
    virtual void handleError(const std::string& msg);

private:
    kmonitor_adapter::Monitor* _monitor = nullptr;
    mutable autil::ThreadMutex _lock;
    int64_t _checkServiceKeeperThreshold;
    int64_t _checkGenerationKeeperThreshold;
    autil::LoopThreadPtr _loopThread;
    std::map<proto::BuildId, GenerationKeeperPtr> _generations;
    ServiceKeeper* _serviceKeeper;
    kmonitor_adapter::MetricPtr _scheduleFreshnessMetric;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(AdminHealthChecker);

}} // namespace build_service::admin

#endif // ISEARCH_BS_ADMINHEALTHCHECKER_H
