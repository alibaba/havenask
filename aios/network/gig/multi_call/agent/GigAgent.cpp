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
#include "aios/network/gig/multi_call/agent/GigAgent.h"
#include "aios/network/gig/multi_call/agent/GigStatistic.h"
#include "aios/network/gig/multi_call/metric/MetricReporterManager.h"
#include <assert.h>

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GigAgent);

GigAgent::GigAgent()
    : _statistic(NULL), _metricReporterManager(new MetricReporterManager()) {}

GigAgent::~GigAgent() {
    DELETE_AND_SET_NULL(_statistic);
    AUTIL_LOG(INFO, "agent[%p] exited", this);
}

bool GigAgent::init(const std::string &logPrefix, bool enableAgentStat) {
    if (enableAgentStat) {
        _statistic = new GigStatistic(logPrefix);
        _statistic->init();
    }
    return _metricReporterManager->init(true);
}

QueryInfoPtr GigAgent::getQueryInfo(const std::string &queryInfo,
                                    const std::string &warmUpStrategy,
                                    bool withBizStat) {
    return QueryInfoPtr(new QueryInfo(queryInfo, warmUpStrategy,
                                      withBizStat ? _statistic : nullptr,
                                      _metricReporterManager));
}

void GigAgent::updateWarmUpStrategy(const std::string &warmUpStrategy,
                                    int64_t timeoutInSecond,
                                    int64_t queryCountLimit) {
    if (_statistic) {
        _statistic->updateWarmUpStrategy(warmUpStrategy, timeoutInSecond,
                                         queryCountLimit);
    }
}

void GigAgent::start() {
    if (_statistic) {
        _statistic->start();
    }
    AUTIL_LOG(INFO, "agent is started");
}

void GigAgent::stop() {
    if (_statistic) {
        _statistic->stop();
    }
    AUTIL_LOG(INFO, "agent is stopped");
}

bool GigAgent::isStopped() const {
    return _statistic ? _statistic->stopped() : true;
}

void GigAgent::start(const std::string &bizName, PartIdTy partId) {
    if (_statistic) {
        _statistic->start(bizName, partId);
    }
    AUTIL_LOG(INFO, "biz[%s] partId [%d] is started", bizName.c_str(), partId);
}

void GigAgent::stop(const std::string &bizName, PartIdTy partId) {
    if (_statistic) {
        _statistic->stop(bizName, partId);
    }
    AUTIL_LOG(INFO, "biz[%s] partId [%d] is stopped", bizName.c_str(), partId);
}

bool GigAgent::isStopped(const std::string &bizName, PartIdTy partId) const {
    return _statistic ? _statistic->stopped(bizName, partId) : true;
}

bool GigAgent::longTimeNoQuery(int64_t noQueryTimeInSecond) const {
    return _statistic ? _statistic->longTimeNoQuery(noQueryTimeInSecond) : true;
}

bool GigAgent::longTimeNoQuery(const std::string &bizName,
                               int64_t noQueryTimeInSecond) const {
    return _statistic
               ? _statistic->longTimeNoQuery(bizName, noQueryTimeInSecond)
               : true;
}

std::shared_ptr<BizStat> GigAgent::getBizStat(const std::string &bizName, PartIdTy partId) {
    if (_statistic) {
        return _statistic->getBizStat(bizName, partId);
    } else {
        return nullptr;
    }
}

} // namespace multi_call
