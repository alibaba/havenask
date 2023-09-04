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
#include "build_service/admin/AdminHealthChecker.h"

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "build_service/admin/GenerationKeeper.h"
#include "build_service/admin/IndexInfoCollector.h"
#include "build_service/admin/ServiceKeeper.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/util/Monitor.h"

using namespace std;
using namespace autil;
using namespace build_service::proto;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, AdminHealthChecker);

AdminHealthChecker::AdminHealthChecker(kmonitor_adapter::Monitor* globalMonitor)
    : _monitor(globalMonitor)
    , _checkServiceKeeperThreshold(std::numeric_limits<int64_t>::max())
    , _checkGenerationKeeperThreshold(std::numeric_limits<int64_t>::max())
{
}

AdminHealthChecker::~AdminHealthChecker()
{
    if (_loopThread) {
        _loopThread->stop();
        _loopThread.reset();
    }
}

bool AdminHealthChecker::init(ServiceKeeper* keeper, int64_t checkInterval)
{
    if (keeper == NULL) {
        return false;
    }
    _serviceKeeper = keeper;

    {
        string param = autil::EnvUtil::getEnv(BS_ENV_HEALTH_CHECK_SERVICEKEEPER_TIMEOUT.c_str());
        int64_t timeout = 0;
        if (!param.empty() && StringUtil::fromString(param, timeout) && timeout > 0) {
            _checkServiceKeeperThreshold = timeout;
        }
        BS_LOG(INFO, "check ServiceKeeper timeout is %ld seconds", _checkServiceKeeperThreshold);
    }
    {
        string param = autil::EnvUtil::getEnv(BS_ENV_HEALTH_CHECK_GENERATIONKEEPER_TIMEOUT.c_str());
        int64_t timeout = 0;
        if (!param.empty() && StringUtil::fromString(param, timeout) && timeout > 0) {
            _checkGenerationKeeperThreshold = timeout;
        }
        BS_LOG(INFO, "check GenerationKeeper timeout is %ld seconds", _checkGenerationKeeperThreshold);
    }

    _loopThread = LoopThread::createLoopThread(bind(&AdminHealthChecker::checkHealth, this), checkInterval);
    if (!_loopThread) {
        return false;
    }

    if (_monitor) {
        _scheduleFreshnessMetric = _monitor->registerGaugeMetric("health.scheduleFreshness", kmonitor::FATAL);
    }
    return true;
}

bool AdminHealthChecker::addGenerationKeeper(const BuildId buildId, const GenerationKeeperPtr& generationKeeper)
{
    ScopedLock lock(_lock);
    assert(generationKeeper);
    auto iter = _generations.find(buildId);
    if (iter != _generations.end()) {
        _generations[buildId] = generationKeeper;
    } else {
        _generations.insert(make_pair(buildId, generationKeeper));
    }
    generationKeeper->updateLastRefreshTime();
    return true;
}

void AdminHealthChecker::checkHealth()
{
    BS_INTERVAL_LOG(60, INFO, "check admin health");
    int64_t lastScheduleTime = _serviceKeeper->getLastScheduleTimestamp();
    int64_t currentTime = TimeUtility::currentTimeInSeconds();
    reportScheduleFreshness(currentTime - lastScheduleTime);

    if (currentTime - _serviceKeeper->getLastRefreshTime() > _checkServiceKeeperThreshold) {
        stringstream ss;
        ss << "ServiceKeeper is blocked, current ts [" << currentTime << "], last refresh ts ["
           << _serviceKeeper->getLastRefreshTime() << "], healthCheckThreshold [" << _checkServiceKeeperThreshold
           << "]";
        BS_LOG(ERROR, "%s", ss.str().c_str());
        cerr << ss.str() << endl;
        handleError(ss.str());
        return;
    }

    ScopedLock lock(_lock);
    vector<BuildId> needToRemove;
    for (auto it = _generations.begin(); it != _generations.end(); it++) {
        auto buildId = it->first;
        GenerationKeeperPtr gKeeper = it->second;
        if (gKeeper->isStopped()) {
            string msg = "generation [" + buildId.ShortDebugString() +
                         "] is stopped, "
                         "remove from health checker.";
            BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, msg, BuildIdWrapper::getEventTags(buildId));
            needToRemove.push_back(buildId);
            continue;
        }

        if (currentTime - gKeeper->getLastRefreshTime() <= _checkGenerationKeeperThreshold) {
            continue;
        }
        stringstream ss;
        ss << "GenerationKeeper[" << buildId.ShortDebugString().c_str() << "] is blocked, current ts[" << currentTime
           << "], last refresh ts[" << gKeeper->getLastRefreshTime() << "], healthCheckThreshold["
           << _checkGenerationKeeperThreshold << "]";
        BS_LOG(ERROR, "%s", ss.str().c_str());
        cerr << ss.str() << endl;
        handleError(ss.str());
    }
    for (size_t i = 0; i < needToRemove.size(); i++) {
        BuildId buildId = needToRemove[i];
        auto it = _generations.find(buildId);
        _generations.erase(it);
    }
    BS_INTERVAL_LOG(10, INFO, "check health end");
}

void AdminHealthChecker::handleError(const std::string& message)
{
    // sleep to wait for flushing log
    BS_LOG_FLUSH();

    string msg = message + ". AdminWorker check health error, i will exit!";
    BEEPER_REPORT(ADMIN_STATUS_COLLECTOR_NAME, message);
    BEEPER_CLOSE();
    _exit(-1);
}

void AdminHealthChecker::reportScheduleFreshness(int64_t value) const
{
    if (_scheduleFreshnessMetric) {
        REPORT_KMONITOR_METRIC(_scheduleFreshnessMetric, value);
    }
}

}} // namespace build_service::admin
