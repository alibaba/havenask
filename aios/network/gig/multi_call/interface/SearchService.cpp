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
#include "aios/network/gig/multi_call/interface/SearchService.h"

#include "aios/network/gig/multi_call/agent/QueryInfo.h"
#include "aios/network/gig/multi_call/interface/SyncClosure.h"
#include "aios/network/gig/multi_call/metric/MetricReporterManager.h"
#include "aios/network/gig/multi_call/proto/GigCallProto.pb.h"
#include "aios/network/gig/multi_call/service/CallDelegationThread.h"
#include "aios/network/gig/multi_call/service/ChildNodeCaller.h"
#include "aios/network/gig/multi_call/service/FlowConfigSnapshot.h"
#include "aios/network/gig/multi_call/service/LatencyTimeSnapshot.h"
#include "aios/network/gig/multi_call/service/RetryLimitChecker.h"
#include "aios/network/gig/multi_call/service/SearchServiceManager.h"
#include "aios/network/gig/multi_call/stream/GigClientStreamImpl.h"
#include "autil/EnvUtil.h"
#include "autil/LoopThread.h"
#include "autil/legacy/any.h"

using namespace std;
using namespace autil::legacy::json;
using namespace autil::legacy;
using namespace autil;

namespace multi_call {

AUTIL_LOG_SETUP(multi_call, SearchService);

SearchService::SearchService() {
    _serviceManager.reset(new SearchServiceManager(this));
    _flowConfigSnapshot.reset(new FlowConfigSnapshot());
    _metricReporterManager.reset(new MetricReporterManager());
    _retryLimitChecker.reset(new RetryLimitChecker());
    _latencyTimeSnapshot.reset(new LatencyTimeSnapshot());
    _serviceManager->setLatencyTimeSnapshot(_latencyTimeSnapshot);
    _versionSelectorFactory.reset(
        new VersionSelectorFactory(_serviceManager->getSubscribeServiceManager()));
    _providerSelectorFactory.reset(new ProviderSelectorFactory());
    _tracerProvider.reset(new opentelemetry::TracerProvider());
}

SearchService::~SearchService() {
    stop();
}

void SearchService::stop() {
    if (_serviceManager) {
        _serviceManager->stop();
        _serviceManager.reset();
    }
    _callThread.reset();
    _flowConfigSnapshot.reset();
    _metricReporterManager.reset();
    _retryLimitChecker.reset();
    _latencyTimeSnapshot.reset();
    _eagleeyeConfig.reset();
    _tracerProvider.reset();
    // AUTIL_LOG(INFO, "search service [%p] stopped", this);
}

bool SearchService::init(const JsonMap &mcConfig) {
    MultiCallConfig multiCallConfig;
    try {
        FromJson(multiCallConfig, mcConfig);
    } catch (const ExceptionBase &e) {
        AUTIL_LOG(WARN, "jsonize multi_call config failed [%s], error [%s]",
                  ToJsonString(mcConfig, true).c_str(), e.what());
        return false;
    }
    return init(multiCallConfig);
}

bool SearchService::init(const MultiCallConfig &mcConfig) {
    // set env GRPC_CLIENT_CHANNEL_BACKUP_POLL_INTERVAL_MS to fix grpc channel
    // connecting state not ready for 5s
    static string GRPC_CLIENT_CHANNEL_BACKUP_POLL_INTERVAL_MS =
        "GRPC_CLIENT_CHANNEL_BACKUP_POLL_INTERVAL_MS";
    if (!autil::EnvUtil::hasEnv(GRPC_CLIENT_CHANNEL_BACKUP_POLL_INTERVAL_MS)) {
        string grpcChannelInterval =
            StringUtil::toString(mcConfig.miscConf.grpcChannelPollIntervalMs);
        autil::EnvUtil::setEnv("GRPC_CLIENT_CHANNEL_BACKUP_POLL_INTERVAL_MS", grpcChannelInterval,
                               false);
    }
    if (!_metricReporterManager->init(false, mcConfig.miscConf.disableMetricReport,
                                      mcConfig.miscConf.simplifyMetricReport)) {
        return false;
    }
    _metricReporterManager->setReportSampling(mcConfig.miscConf.metricReportSamplingRate);
    _serviceManager->setMetricReporterManager(_metricReporterManager);
    if (!_serviceManager->init(mcConfig)) {
        AUTIL_LOG(ERROR, "ServiceManager init failed");
        return false;
    }
    if (!restartCallThread(mcConfig.miscConf.callProcessInterval,
                           mcConfig.miscConf.callDelegationQueueSize)) {
        AUTIL_LOG(ERROR, "start call delegation thread failed");
        return false;
    }

    if (!_tracerProvider->init(mcConfig.traceConf)) {
        AUTIL_LOG(ERROR, "TracerProvider init failed");
        return false;
    }
    _config = mcConfig;
    AUTIL_LOG(INFO, "init search service [%p] success, config[%s]", this,
              ToJsonString(_config, true).c_str());
    return true;
}

bool SearchService::addSubscribe(const SubscribeClustersConfig &gigSubConf) {
    return _serviceManager->addSubscribe(gigSubConf);
}

bool SearchService::deleteSubscribe(const SubscribeClustersConfig &gigSubConf) {
    return _serviceManager->deleteSubscribe(gigSubConf);
}

void SearchService::enableSubscribeService(SubscribeType ssType) {
    _serviceManager->enableSubscribeService(ssType);
}

void SearchService::disableSubscribeService(SubscribeType ssType) {
    _serviceManager->disableSubscribeService(ssType);
}

void SearchService::stopSubscribeService(SubscribeType ssType) {
    _serviceManager->stopSubscribeService(ssType);
}

void SearchService::waitCreateSnapshotLoop() {
    _serviceManager->waitCreateSnapshotLoop();
}

bool SearchService::updateFlowConfig(const string &flowConfigStrategy, const JsonMap *flowConf) {
    FlowControlConfigPtr flowControlConfig;
    if (flowConf) {
        try {
            flowControlConfig.reset(new FlowControlConfig());
            FromJson(*flowControlConfig, *flowConf);
        } catch (const ExceptionBase &e) {
            string jsonStr;
            ToString(*flowConf, jsonStr, true);
            AUTIL_LOG(WARN, "init flow config failed [%s], error [%s]", jsonStr.c_str(), e.what());
            return false;
        }
    }
    return updateFlowConfig(flowConfigStrategy, flowControlConfig);
}

bool SearchService::updateFlowConfig(const std::string &flowConfigStrategy,
                                     const FlowControlConfigPtr &flowControlConfig) {
    FlowConfigSnapshotPtr newSnapshot = getFlowConfigSnapshot()->clone();
    newSnapshot->updateFlowConfig(flowConfigStrategy, flowControlConfig);
    setFlowConfigSnapshot(newSnapshot);
    return true;
}

void SearchService::updateFlowConfig(
    const std::map<std::string, FlowControlConfigPtr> &flowControlConfigMap) {
    FlowConfigSnapshotPtr newSnapshot = getFlowConfigSnapshot()->clone();
    for (const auto &configPair : flowControlConfigMap) {
        newSnapshot->updateFlowConfig(configPair.first, configPair.second);
    }
    setFlowConfigSnapshot(newSnapshot);
}

bool SearchService::updateDefaultFlowConfig(const FlowControlConfigPtr &flowControlConfig) {
    FlowConfigSnapshotPtr newSnapshot = getFlowConfigSnapshot()->clone();
    newSnapshot->updateDefaultFlowConfig(flowControlConfig);
    setFlowConfigSnapshot(newSnapshot);
    return true;
}

FlowConfigSnapshotPtr SearchService::getFlowConfigSnapshot() {
    ScopedReadWriteLock lock(_snapshotLock, 'r');
    return _flowConfigSnapshot;
}

FlowControlConfigMap SearchService::getFlowConfigMap() {
    auto snapshot = getFlowConfigSnapshot();
    if (snapshot) {
        return snapshot->getConfigMap();
    }
    return {};
}

void SearchService::setFlowConfigSnapshot(const FlowConfigSnapshotPtr &flowConfigSnapshot) {
    AUTIL_LOG(INFO, "update FlowConfigSnapshot: %s", flowConfigSnapshot->toString().c_str());
    ScopedReadWriteLock lock(_snapshotLock, 'w');
    _flowConfigSnapshot = flowConfigSnapshot;
}

FlowControlConfigPtr SearchService::getFlowConfig(const std::string &flowConfigStrategy) {
    return getFlowConfigSnapshot()->getFlowControlConfig(flowConfigStrategy);
}

void SearchService::search(const SearchParam &param, ReplyPtr &reply) {
    QuerySessionPtr session(new QuerySession(this->shared_from_this()));
    search(param, reply, session);
}

void SearchService::search(const SearchParam &param, ReplyPtr &reply,
                           const QuerySessionPtr &querySession) {
    reply.reset(new Reply());
    auto searchSnapshot = _serviceManager->getSearchServiceSnapshot();
    auto flowSnapshot = getFlowConfigSnapshot();
    ChildNodeCaller childNodeCaller(searchSnapshot, flowSnapshot, _callThread, param.generatorVec,
                                    _retryLimitChecker, _latencyTimeSnapshot, querySession);
    auto childNodeReply = childNodeCaller.call();
    childNodeReply->setMetricReportManager(_metricReporterManager);
    reply->setChildNodeReply(childNodeReply);
    if (param.closure) {
        childNodeCaller.afterCall(param.closure);
    } else {
        SyncClosure syncClosure;
        childNodeCaller.afterCall(&syncClosure);
        syncClosure.wait();
    }
}

vector<string> SearchService::getBizNames() const {
    auto searchSnapshot = _serviceManager->getSearchServiceSnapshot();
    if (!searchSnapshot) {
        return vector<string>();
    }
    return searchSnapshot->getBizNames();
}

int32_t SearchService::getBizPartCount(const std::string &bizName) const {
    std::vector<multi_call::BizInfo> bizInfos;
    getBizInfos(bizName, bizInfos);
    if (bizInfos.empty()) {
        return 0;
    }
    return bizInfos[0].part_count();
}

bool SearchService::bind(const GigClientStreamPtr &stream) {
    if (!stream) {
        return false;
    }
    QuerySessionPtr session(new QuerySession(shared_from_this()));
    return bind(stream, session);
}

bool SearchService::bind(const GigClientStreamPtr &stream, const QuerySessionPtr &session) {
    if (!stream) {
        return false;
    }
    auto searchSnapshot = _serviceManager->getSearchServiceSnapshot();
    if (!searchSnapshot) {
        AUTIL_LOG(ERROR, "null snapshot");
        return false;
    }
    return bind(stream, session, searchSnapshot);
}

bool SearchService::bind(const GigClientStreamPtr &stream,
                         const SearchServiceSnapshotPtr &searchSnapshot) {
    QuerySessionPtr session(new QuerySession(nullptr));
    return bind(stream, session, searchSnapshot);
}

bool SearchService::bind(const GigClientStreamPtr &stream, const QuerySessionPtr &session,
                         const SearchServiceSnapshotPtr &searchSnapshot) {
    auto generator = dynamic_pointer_cast<RequestGenerator>(stream);
    auto flowSnapshot = getFlowConfigSnapshot();
    ChildNodeCaller childNodeCaller(searchSnapshot, flowSnapshot, _callThread, {generator},
                                    _retryLimitChecker, _latencyTimeSnapshot, session);
    SearchServiceResourceVector resourceVec;
    auto childNodeReply = childNodeCaller.call(resourceVec);
    childNodeReply->setMetricReportManager(_metricReporterManager);
    auto disableRetry = !childNodeCaller.isRetryOn();
    auto forceStop = stream->getForceStop();
    auto streamImpl = stream->getImpl();
    return streamImpl->init(childNodeReply, resourceVec, childNodeCaller.getCaller(), disableRetry,
                            forceStop);
}

bool SearchService::setSnapshotChangeCallback(SnapshotChangeCallback *callback) {
    return _serviceManager->setSnapshotChangeCallback(callback);
}

void SearchService::stealSnapshotChangeCallback() {
    _serviceManager->stealSnapshotChangeCallback();
}

bool SearchService::hasCluster(const std::string &clusterName) const {
    auto searchSnapshot = _serviceManager->getSearchServiceSnapshot();
    if (!searchSnapshot) {
        return false;
    }
    return searchSnapshot->hasCluster(clusterName);
}

bool SearchService::hasBiz(const string &bizName) const {
    auto searchSnapshot = _serviceManager->getSearchServiceSnapshot();
    if (!searchSnapshot) {
        return false;
    }
    return searchSnapshot->hasBiz(bizName);
}

bool SearchService::hasBiz(const string &clusterName, const string &bizName) const {
    auto name = MiscUtil::createBizName(clusterName, bizName);
    return hasBiz(name);
}

bool SearchService::hasVersion(const std::string &bizName, VersionTy version,
                               VersionInfo &info) const {
    return hasVersion(EMPTY_STRING, bizName, version, info);
}

bool SearchService::hasVersion(const std::string &clusterName, const std::string &bizName,
                               VersionTy version, VersionInfo &info) const {
    auto searchSnapshot = _serviceManager->getSearchServiceSnapshot();
    if (!searchSnapshot) {
        info.clear();
        return false;
    }
    auto name = MiscUtil::createBizName(clusterName, bizName);
    auto bizSnapshot = searchSnapshot->getBizSnapshot(name);
    if (!bizSnapshot) {
        if (clusterName.empty()) {
            _serviceManager->getSubscribeServiceManager()->subscribeBiz(bizName);
        }
        info.clear();
        return false;
    }
    return bizSnapshot->hasVersion(version, info);
}

std::vector<VersionTy> SearchService::getBizVersion(const std::string &bizName) const {
    auto searchSnapshot = _serviceManager->getSearchServiceSnapshot();
    if (!searchSnapshot) {
        return vector<VersionTy>();
    }
    return searchSnapshot->getBizVersion(bizName);
}

std::set<VersionTy> SearchService::getBizProtocalVersion(const std::string &bizName) const {
    auto searchSnapshot = _serviceManager->getSearchServiceSnapshot();
    if (!searchSnapshot) {
        return std::set<VersionTy>();
    }
    return searchSnapshot->getBizProtocalVersion(bizName);
}

bool SearchService::getBizInfos(const std::string &bizName, std::vector<BizInfo> &bizInfos) const {
    bizInfos.clear();
    auto searchSnapshot = _serviceManager->getSearchServiceSnapshot();
    if (!searchSnapshot) {
        return false;
    }
    auto bizSnapshot = searchSnapshot->getBizSnapshot(bizName);
    if (!bizSnapshot) {
        return false;
    }
    const auto &versionSnapshotMap = bizSnapshot->getVersionSnapshotMap();
    if (versionSnapshotMap.empty()) {
        return false;
    }
    for (const auto &snapshot : versionSnapshotMap) {
        BizInfo info;
        info.set_biz_name(bizName);
        info.set_part_count(snapshot.second->getPartCount());
        info.set_version(snapshot.second->getVersion());
        info.set_is_complete(snapshot.second->isComplete());
        bizInfos.emplace_back(info);
    }
    return true;
}

bool SearchService::getBizMetaInfos(std::vector<BizMetaInfo> &bizMetaInfos) const {
    bizMetaInfos.clear();
    auto searchSnapshot = _serviceManager->getSearchServiceSnapshot();
    if (!searchSnapshot) {
        return false;
    }
    searchSnapshot->getBizMetaInfos(bizMetaInfos);
    return true;
}

std::shared_ptr<SearchServiceSnapshot> SearchService::getSearchServiceSnapshot() {
    return _serviceManager->getSearchServiceSnapshot();
}

bool SearchService::restartCallThread(int64_t processInterval, uint64_t queueSize) {
    _callThread.reset();
    _callThread.reset(new CallDelegationThread(_serviceManager,
                                               _metricReporterManager->getWorkerMetricReporter(),
                                               processInterval, queueSize));
    if (!_callThread->start()) {
        AUTIL_LOG(WARN, "restart call delegation thread failed");
        _callThread.reset();
        return false;
    }
    return true;
}

int64_t SearchService::getCallProcessInterval() const {
    if (!_callThread) {
        return -1;
    }
    return _callThread->getProcessInterval();
}

void SearchService::disableSnapshotLog() {
    _serviceManager->stopLogThread();
}

} // namespace multi_call
