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
#ifndef ISEARCH_MULTI_CALL_SEARCHSERVICE_H
#define ISEARCH_MULTI_CALL_SEARCHSERVICE_H

#include "aios/network/gig/multi_call/common/VersionInfo.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/config/FlowControlConfig.h"
#include "aios/network/gig/multi_call/config/MultiCallConfig.h"
#include "aios/network/gig/multi_call/config/SubscribeClustersConfig.h"
#include "aios/network/gig/multi_call/interface/Closure.h"
#include "aios/network/gig/multi_call/interface/Reply.h"
#include "aios/network/gig/multi_call/interface/RequestGenerator.h"
#include "aios/network/gig/multi_call/stream/GigClientStream.h"
#include "aios/network/opentelemetry/core/TracerProvider.h"
#include "autil/legacy/json.h"

namespace multi_call {

class BizInfo;
struct CallParam;
typedef CallParam SearchParam;
class SearchServiceSnapshot;
class SearchServiceManager;
MULTI_CALL_TYPEDEF_PTR(SearchServiceManager);
class MetricReporterManager;
MULTI_CALL_TYPEDEF_PTR(MetricReporterManager);
class CallDelegationThread;
MULTI_CALL_TYPEDEF_PTR(CallDelegationThread);
class VersionSelectorFactory;
MULTI_CALL_TYPEDEF_PTR(VersionSelectorFactory);
class ProviderSelectorFactory;
MULTI_CALL_TYPEDEF_PTR(ProviderSelectorFactory);
class FlowConfigSnapshot;
MULTI_CALL_TYPEDEF_PTR(FlowConfigSnapshot);

class QuerySession;
MULTI_CALL_TYPEDEF_PTR(QuerySession);
class RetryLimitChecker;
MULTI_CALL_TYPEDEF_PTR(RetryLimitChecker);
class LatencyTimeSnapshot;
MULTI_CALL_TYPEDEF_PTR(LatencyTimeSnapshot);

class SearchService : public std::enable_shared_from_this<SearchService> {
public:
    SearchService();
    virtual ~SearchService();

private:
    SearchService(const SearchService &);
    SearchService &operator=(const SearchService &);

public:
    virtual bool init(const autil::legacy::json::JsonMap &mcConfig);
    virtual bool init(const MultiCallConfig &mcConfig);

    virtual void search(const SearchParam &param, ReplyPtr &reply);
    virtual void search(const SearchParam &param, ReplyPtr &reply,
                        const QuerySessionPtr &querySession);

public: // config update
    virtual bool updateFlowConfig(const std::string &flowConfigStrategy,
                                  const autil::legacy::json::JsonMap *flowConf);
    virtual bool
    updateFlowConfig(const std::string &flowConfigStrategy,
                     const FlowControlConfigPtr &flowControlConfig);
    virtual void
    updateFlowConfig(const std::map<std::string, FlowControlConfigPtr>
                         &flowControlConfigMap);
    virtual bool
    updateDefaultFlowConfig(const FlowControlConfigPtr &flowControlConfig);

    void setEagleeyeConfig(const EagleeyeConfig &eagleeyeConfig) {
        _eagleeyeConfig.reset(new EagleeyeConfig(eagleeyeConfig));
    }
    const EagleeyeConfigPtr &getEagleeyeConfig() const {
        return _eagleeyeConfig;
    }
    virtual const MultiCallConfig &getMultiCallConfig() const {
        return _config;
    }
    FlowControlConfigPtr getFlowConfig(const std::string &flowConfigStrategy);

public: // subscribe
    void enableSubscribeService(SubscribeType ssType);
    void disableSubscribeService(SubscribeType ssType);
    void stopSubscribeService(SubscribeType ssType);
    bool addSubscribe(const SubscribeClustersConfig &gigSubConf);
    bool deleteSubscribe(const SubscribeClustersConfig &gigSubConf);

public: // biz & version & cluster
    virtual std::vector<std::string> getBizNames() const;
    virtual int32_t getBizPartCount(const std::string &bizName) const;
    bool hasCluster(const std::string &clusterName) const;
    virtual bool hasBiz(const std::string &bizName) const;
    virtual bool hasBiz(const std::string &clusterName,
                        const std::string &bizName) const;
    bool hasVersion(const std::string &bizName, VersionTy version,
                    VersionInfo &info) const;
    bool hasVersion(const std::string &clusterName, const std::string &bizName,
                    VersionTy version, VersionInfo &info) const;
    virtual std::vector<VersionTy>
    getBizVersion(const std::string &bizName) const;
    virtual std::set<VersionTy>
    getBizProtocalVersion(const std::string &bizName) const;
    bool getBizInfos(const std::string &bizName,
                     std::vector<BizInfo> &bizInfos) const;
    FlowControlConfigMap getFlowConfigMap();
public: // call in gig
    FlowConfigSnapshotPtr getFlowConfigSnapshot();
    std::shared_ptr<SearchServiceSnapshot> getSearchServiceSnapshot();
    const std::shared_ptr<VersionSelectorFactory> &
    getVersionSelectorFactory() const {
        return _versionSelectorFactory;
    }
    const std::shared_ptr<ProviderSelectorFactory> &
    getProviderSelectorFactory() const {
        return _providerSelectorFactory;
    }
    const RetryLimitCheckerPtr &getRetryLimitChecker() const {
        return _retryLimitChecker;
    }
    const LatencyTimeSnapshotPtr &getLatencyTimeSnapshot() const {
        return _latencyTimeSnapshot;
    }
    const opentelemetry::TracerProviderPtr &getTracerProvider() const {
        return _tracerProvider;
    }
    const std::shared_ptr<SearchServiceManager> &
    getSearchServiceManager() const {
        return _serviceManager;
    }
    const std::shared_ptr<MetricReporterManager> &
    getMetricReporterManager() const {
        return _metricReporterManager;
    }
    void disableSnapshotLog();

public:
    virtual bool restartCallThread(
        int64_t processInterval,
        uint64_t queueSize = std::numeric_limits<uint64_t>::max());
    virtual int64_t getCallProcessInterval() const;
    void waitCreateSnapshotLoop();
    bool bind(const GigClientStreamPtr &stream);
    bool bind(const GigClientStreamPtr &stream, const QuerySessionPtr &session);
    bool bind(const GigClientStreamPtr &stream,
              const std::shared_ptr<SearchServiceSnapshot> &searchSnapshot);
private:
    bool bind(const GigClientStreamPtr &stream, const QuerySessionPtr &session,
              const std::shared_ptr<SearchServiceSnapshot> &searchSnapshot);
    void setFlowConfigSnapshot(const FlowConfigSnapshotPtr &flowConfigSnapshot);
    void stop();

private:
    MultiCallConfig _config;
    EagleeyeConfigPtr _eagleeyeConfig;

    autil::ReadWriteLock _snapshotLock;
    FlowConfigSnapshotPtr _flowConfigSnapshot;
    LatencyTimeSnapshotPtr _latencyTimeSnapshot;

    SearchServiceManagerPtr _serviceManager;
    MetricReporterManagerPtr _metricReporterManager;
    CallDelegationThreadPtr _callThread;
    RetryLimitCheckerPtr _retryLimitChecker;
    VersionSelectorFactoryPtr _versionSelectorFactory;
    ProviderSelectorFactoryPtr _providerSelectorFactory;
    opentelemetry::TracerProviderPtr _tracerProvider;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(SearchService);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_SEARCHSERVICE_H
