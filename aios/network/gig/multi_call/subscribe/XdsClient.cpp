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
#include "aios/network/gig/multi_call/subscribe/XdsClient.h"
#include "aios/network/gig/multi_call/subscribe/XdsStore.h"
#include "autil/NetUtil.h"
#include "autil/WorkItem.h"
#include <unistd.h>

using namespace std;
using namespace autil;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, XdsClient);

const string XdsClient::kCDSUrl = "type.googleapis.com/envoy.api.v2.Cluster";
const string XdsClient::kEDSUrl =
    "type.googleapis.com/envoy.api.v2.ClusterLoadAssignment";

typedef std::function<void(void)> ReqMethod;
class DiscoveryResWorkItem : public autil::WorkItem {
public:
    DiscoveryResWorkItem(ReqMethod &req) : _reqMethod(req) {}
    ~DiscoveryResWorkItem() {}

public:
    void process() override;

private:
    ReqMethod _reqMethod;
};

void DiscoveryResWorkItem::process() { _reqMethod(); }

XdsClient::XdsClient(const IstioConfig &config,
                     const IstioMetricReporterPtr reporter,
                     std::shared_ptr<XdsStore> xdsStore)
    : _config(config), _metricReporter(reporter), _xdsStore(xdsStore) {
    _CDSsyncedTimestamp = 0;
    _EDSsyncedTimestamp = 0;
    _CDSRequestTimestamp = 0;
    _EDSRequestTimestamp = 0;
    _stop = false;
}
XdsClient::~XdsClient() {}

bool XdsClient::checkSubscribeWork() {
    return _CDSsyncedTimestamp || _EDSsyncedTimestamp;
}

bool XdsClient::isSynced() {
    bool synced = (_CDSsyncedTimestamp && _EDSsyncedTimestamp);
    if (synced) {
        return true;
    }
    {
        autil::ScopedLock lock(_mutex);
        if (_watchedResourceNames.empty())
            synced = true;
    }
    return synced;
}

bool XdsClient::addSubscribe(const std::vector<std::string> &names) {
    std::set<std::string> resources = XdsStore::hostnamesToResources(names);
    bool changed = false;
    {
        autil::ScopedLock lock(_mutex);
        for (const string &r : resources) {
            if (_watchedResourceNames.count(r) == 1) {
                continue;
            }
            _watchedResourceNames.insert(r);
            changed = true;
        }
    }
    if (!changed) {
        return true;
    }
    return reWatch(_config.asyncSubscribe);
};

bool XdsClient::deleteSubscribe(const std::vector<std::string> &names) {
    std::set<std::string> resources = XdsStore::hostnamesToResources(names);
    bool changed = false;
    std::set<std::string> deletedResource;
    {
        autil::ScopedLock lock(_mutex);
        for (const string &r : resources) {
            if (_watchedResourceNames.count(r) == 0) {
                continue;
            }
            _watchedResourceNames.erase(r);
            changed = true;
            deletedResource.insert(r);
        }
    }
    if (!changed) {
        return true;
    }
    // delete CDS/EDS for un-watched clusters
    for (const string &r : deletedResource) {
        string hostname = XdsStore::resourceToHostname(r);
        _xdsStore->clear(hostname);
    }
    return reWatch(_config.asyncSubscribe);
};

bool XdsClient::parseVersionInfo(const string &str,
                                 IstioVersionInfo *versioninfo) {
    std::vector<std::string> vec = autil::StringUtil::split(str, "/");
    if (vec.size() != 3) {
        return false;
    }
    if (vec[0] == "F") {
        versioninfo->full = true;
    } else {
        versioninfo->full = false;
    }
    versioninfo->timestamp = vec[1];
    versioninfo->serverVersion =
        autil::StringUtil::strToUInt64WithDefault(vec[2].c_str(), 0);
    return true;
}

bool XdsClient::init() {
    std::string localIP;
    if (!autil::NetUtil::GetDefaultIp(localIP)) {
        AUTIL_LOG(ERROR, "Error on get local ip: hippoSlaveIp is null");
        return false;
    }
    if (_config.istioHost.empty()) {
        AUTIL_LOG(ERROR, "Error isito config: _config.istioHost is null");
        return false;
    }
    // init grpcAdsClient
    // TODO(guanming.fh) support multi-servers && add app name to nodeid
    std::vector<std::string> pilotServers = { _config.istioHost };
    std::string nodeid = "sidecar~" + localIP + "~v3~istio.local";
    AUTIL_LOG(INFO, "nodeid: [%s] ", nodeid.c_str());

    _grpcAdsClient.reset(new GrpcAdsClient(nodeid, pilotServers));
    if (!_grpcAdsClient) {
        AUTIL_LOG(ERROR,
                  "Fail on creating grpc client nodeid [%s], server [%s].",
                  nodeid.c_str(), _config.istioHost.c_str())
        return false;
    }
    if (!_grpcAdsClient->BuildStream()) {
        AUTIL_LOG(ERROR,
                  "Fail on build grpc stream. Try to load cache if has.");
        if (_config.readCache && !_xdsStore->loadCache()) {
            AUTIL_LOG(ERROR, "Fail to load Cache.")
            return false;
        }
    }
    // init WatchResources
    {
        autil::ScopedLock lock(_mutex);
        _watchedResourceNames =
            XdsStore::hostnamesToResources(_config.clusters);
    }
    // init threads
    if (_config.asyncSubscribe) {
        _subscribeThread = autil::Thread::createThread(
            std::bind(&XdsClient::asyncSubThreadFunc, this), "XdsCliAsyncSub");
        if (!_subscribeThread) {
            AUTIL_LOG(ERROR,
                      "Failed: create xds client async subscriber thread!");
            return false;
        }
    }
    _workerThreads.reset(new autil::ThreadPool(
        _config.xdsClientWorkerThreadNumber, _config.xdsCallbackQueueSize));
    if (!_workerThreads->start("XdsClientWorker")) {
        AUTIL_LOG(ERROR, "Failed: start xds client worker threads");
        return false;
    }
    _receiveThread = autil::Thread::createThread(
        std::bind(&XdsClient::receiveThreadFunc, this), "XdsClientReceive");
    if (!_receiveThread) {
        AUTIL_LOG(ERROR, "Failed: create xds client receive thread!");
        return false;
    }
    _metricCollectorThread = autil::Thread::createThread(
        std::bind(&XdsClient::metricCollector, this), "metricCollector");
    if (!_metricCollectorThread) {
        AUTIL_LOG(ERROR, "Failed: create metric collection thread!");
    }
    return true;
}

bool XdsClient::stopSubscribe() {
    AUTIL_LOG(INFO, "Called XdsClient stopsubscribe!");
    _stop = true;
    if (_grpcAdsClient)
        _grpcAdsClient->TryCancel();
    if (_receiveThread) {
        _receiveThread->join();
        _receiveThread.reset();
    }
    if (_workerThreads) {
        _workerThreads->stop();
        _workerThreads.reset();
    }
    if (_metricCollectorThread) {
        _metricCollectorThread->join();
        _metricCollectorThread.reset();
    }
    _grpcAdsClient.reset();
    _CDSsyncedTimestamp = 0;
    _EDSsyncedTimestamp = 0;
    return true;
}

bool XdsClient::reWatch(bool async) {
    if (async == true) {
        autil::ScopedLock lock(_mutex);
        _needSendSubRequest = true;
        return true;
    }
    std::set<std::string> watchedResourceNames;
    {
        autil::ScopedLock lock(_mutex);
        watchedResourceNames = _watchedResourceNames;
    }
    if (!_grpcAdsClient) {
        AUTIL_LOG(ERROR, "Error xdsclient has empty _grpcAdsClient.");
        return false;
    }
    bool ret =
        _grpcAdsClient->SendDiscoveryRequest(kCDSUrl, watchedResourceNames) &&
        _grpcAdsClient->SendDiscoveryRequest(kEDSUrl, watchedResourceNames);
    _CDSRequestTimestamp = autil::TimeUtility::currentTime();
    _EDSRequestTimestamp = _CDSRequestTimestamp.load();
    if (ret == false) {
        AUTIL_LOG(ERROR, "Failed subscribe clusters len [%lu]",
                  watchedResourceNames.size());
    } else if (_config.asyncSubscribe) {
        autil::ScopedLock lock(_mutex);
        _needSendSubRequest = false;
    }
    return ret;
}

// fallBackSeconds is a helper function to calculate fallback sleep seconds, up
// to 64s.
int fallBackSeconds(int retry) {
    if (retry <= 0)
        return 0;
    else if (retry > 7)
        return 64;
    return 1 << (retry - 1);
}

void XdsClient::receiveThreadFunc() {
    AUTIL_LOG(INFO, "Start XdsClient received thread!");
    if (!_grpcAdsClient) {
        AUTIL_LOG(ERROR,
                  "Error: _grpcAdsClient should not be null after init()!");
        return;
    }
    vector<string> results = { "succ", "fail" };
    vector<string> ops = { "stream", "subscribe", "readrsp" };
    map<string, map<string, kmonitor::MetricsTagsPtr> > tags;
    if (_metricReporter) {
        for (auto &r : results) {
            for (auto &op : ops) {
                tags[r][op] = _metricReporter->GetMetricsTags(
                    { { "result", r }, { "op_type", op } });
            }
        }
    }
    // need build connection with xds server
    bool streamSuccess = true;
    // need send subscription to xds server
    bool subscribeSuccess = false;

    int retryCount = 0;
    while (!_stop) {
        usleep(1000 * 1000 * fallBackSeconds(retryCount)); // 1000 ms * seconds
        if (!streamSuccess) {
            if (!_grpcAdsClient->BuildStream()) {
                AUTIL_LOG(ERROR, "build grpc stream failed. retrying...");
                if (_metricReporter) {
                    _metricReporter->reportXdsBuildStream(
                        1, tags["fail"]["stream"]);
                }
                ++retryCount;
                continue;
            }
            if (_metricReporter) {
                _metricReporter->reportXdsBuildStream(1,
                                                      tags["succ"]["stream"]);
            }
            streamSuccess = true;
            AUTIL_LOG(INFO, "build grpc stream success.");
        }
        if (!subscribeSuccess) {
            if (!reWatch(false)) {
                streamSuccess = false;
                subscribeSuccess = false;
                ++retryCount;
                if (_metricReporter) {
                    _metricReporter->reportXdsBuildStream(
                        1, tags["fail"]["subscribe"]);
                }
                continue;
            }
            if (_metricReporter) {
                _metricReporter->reportXdsBuildStream(
                    1, tags["succ"]["subscribe"]);
            }
            subscribeSuccess = true;
        }
        auto resp = _grpcAdsClient->ReadDiscoveryResponse();
        if (!resp) {
            AUTIL_LOG(ERROR, "Failed read xds response! try to reconnect...");
            streamSuccess = false;
            subscribeSuccess = false;
            ++retryCount;
            if (_metricReporter) {
                _metricReporter->reportXdsBuildStream(1,
                                                      tags["fail"]["readrsp"]);
            }
            continue;
        }
        retryCount = 0;
        if (_metricReporter) {
            _metricReporter->reportXdsBuildStream(1, tags["succ"]["readrsp"]);
        }
        AUTIL_LOG(DEBUG, "msg from server [%s].", resp->DebugString().c_str());
        ReqMethod fn = [this, resp]() { this->processXdsResponse(resp); };
        _workerThreads->pushWorkItem(new DiscoveryResWorkItem(fn));
    }
}

void XdsClient::asyncSubThreadFunc() {
    AUTIL_LOG(INFO, "Start XdsClient async subscribe thread!");
    if (!_grpcAdsClient) {
        AUTIL_LOG(ERROR,
                  "Error: _grpcAdsClient should not be null after init()!");
        return;
    }
    int64_t last = autil::TimeUtility::currentTime();
    int64_t interval = _config.asyncSubIntervalSec * 1000 * 1000;
    int64_t now = autil::TimeUtility::currentTime();
    while (!_stop) {
        now = autil::TimeUtility::currentTime();
        if (now >= last + interval) {
            bool needSendSubRequest;
            {
                autil::ScopedLock lock(_mutex);
                needSendSubRequest = _needSendSubRequest;
            }
            if (needSendSubRequest) {
                bool succ = reWatch(false);
                if (succ) {
                    last = now;
                } else {
                    AUTIL_LOG(ERROR, "Failed: async reWatch.");
                }
            } else {
                last = now;
            }
        }
        usleep(1000 * 1000); // 1000 ms
    }
}

bool XdsClient::xdsResponseSanityCheck(const IstioVersionInfo &vinfo,
                                       const string &typeUrl,
                                       const string &nonce) {
    if (typeUrl != kCDSUrl && typeUrl != kEDSUrl) {
        AUTIL_LOG(ERROR, " typeUrl [%s] not supported.", typeUrl.c_str());
        return false;
    }
    if (!vinfo.full && typeUrl == kCDSUrl) {
        uint64_t cur = _CDSsyncedTimestamp.load();
        if (cur == 0) {
            AUTIL_LOG(ERROR,
                      "CDS received incrmental Failed on parse versionInfo "
                      "timestamp [%s], "
                      "discard message nonce "
                      "[%s] typeUrl [%s].",
                      vinfo.timestamp.c_str(), nonce.c_str(), typeUrl.c_str());
            if (_metricReporter) {
                std::map<std::string, std::string> tags = {
                    { "xdsType", "cds" }, { "errorType", "fullMiss" }
                };
                _metricReporter->reportXdsRspCheck(1, tags);
            }
            return false;
        } else if (cur > vinfo.serverVersion) {
            // Note (guanming.fh) we disable this comments because the
            // versionversion is not timestamp and _CDSsyncedTimestamp is the
            // timestamp of processing message. They should not match because
            // (1) server may change so server version is not consistent (2)
            // server timestamp is not necessry smaller than the client local
            // timestamp.

            // AUTIL_LOG(WARN,
            //           "CDS received incrmental older version versionInfo
            //           [%lu]" "ts [%s], " "nonce [%s] typeUrl [%s]. current
            //           version [%lu]", vinfo.serverVersion,
            //           vinfo.timestamp.c_str(), nonce.c_str(),
            //           typeUrl.c_str(), cur);
            if (_metricReporter) {
                std::map<std::string, std::string> tags = {
                    { "xdsType", "cds" }, { "errorType", "versionMismatch" }
                };
                _metricReporter->reportXdsRspCheck(1, tags);
            }
            // donot reject this msg
            return true;
        }
    }
    if (!vinfo.full && typeUrl == kEDSUrl) {
        uint64_t cur = _EDSsyncedTimestamp.load();
        if (cur == 0) {
            AUTIL_LOG(ERROR,
                      "EDS received incrmental Failed on parse versionInfo "
                      "ts [%s], "
                      "discard message nonce "
                      "[%s] typeUrl [%s].",
                      vinfo.timestamp.c_str(), nonce.c_str(), typeUrl.c_str());
            if (_metricReporter) {
                std::map<std::string, std::string> tags = {
                    { "xdsType", "eds" }, { "errorType", "fullMiss" }
                };
                _metricReporter->reportXdsRspCheck(1, tags);
            }
            return false;
        } else if (cur > vinfo.serverVersion) {
            // AUTIL_LOG(WARN,
            //           "EDS received incrmental older version versionInfo
            //           [%lu] " "ts [%s], " "nonce [%s] typeUrl [%s]. current
            //           version [%lu]", vinfo.serverVersion,
            //           vinfo.timestamp.c_str(), nonce.c_str(),
            //           typeUrl.c_str(), cur);
            if (_metricReporter) {
                std::map<std::string, std::string> tags = {
                    { "xdsType", "eds" }, { "errorType", "versionMismatch" }
                };
                _metricReporter->reportXdsRspCheck(1, tags);
            }
            // donot reject this msg
            return true;
        }
    }
    return true;
}

string bool2String(bool a) { return a ? "true" : "false"; }

void XdsClient::processXdsResponse(
    std::shared_ptr<envoy::api::v2::DiscoveryResponse> resp) {
    AUTIL_LOG(DEBUG, "XdsClient::processXdsResponse response [%s]",
              resp->DebugString().c_str());
    const string &typeUrl = resp->type_url();
    const string &nonce = resp->nonce();
    const string &versionInfo = resp->version_info();
    // const auto &resources = resp->resources();
    IstioVersionInfo vinfo;
    if (!parseVersionInfo(versionInfo, &vinfo)) {
        AUTIL_LOG(ERROR,
                  "Failed on parse versionInfo [%s], discard message nonce "
                  "[%s] typeUrl [%s].",
                  versionInfo.c_str(), nonce.c_str(), typeUrl.c_str());
        return;
    }
    if (!xdsResponseSanityCheck(vinfo, typeUrl, nonce)) {
        return;
    }
    bool success = false;
    std::map<std::string, std::string> tags;
    if (typeUrl == kCDSUrl) {
        tags["xdsType"] = "cds";
        if (vinfo.full && _CDSRequestTimestamp != 0) {
            AUTIL_LOG(INFO, "Received first full CDS response. ");
            if (_metricReporter) {
                _metricReporter->reportXdsRequestLatency(
                    autil::TimeUtility::currentTime() - _CDSRequestTimestamp,
                    tags);
            }
            _CDSRequestTimestamp = 0;
        }
        success = _xdsStore->updateCDS(vinfo.full, resp);
        if (success) {
            autil::ScopedLock lock(_mutex);
            _CDSsyncedTimestamp = autil::TimeUtility::currentTime();
        }
    }
    if (typeUrl == kEDSUrl) {
        tags["xdsType"] = "eds";
        if (vinfo.full && _EDSRequestTimestamp != 0) {
            AUTIL_LOG(INFO, "Received first full EDS response. ");
            if (_metricReporter) {
                _metricReporter->reportXdsRequestLatency(
                    autil::TimeUtility::currentTime() - _EDSRequestTimestamp,
                    tags);
            }
            _EDSRequestTimestamp = 0;
        }
        success = _xdsStore->updateEDS(vinfo.full, resp);
        if (success) {
            {
                autil::ScopedLock lock(_mutex);
                _EDSsyncedTimestamp = autil::TimeUtility::currentTime();
            }
        }
    }
    // report
    tags["result"] = bool2String(success);
    if (_metricReporter) {
        _metricReporter->reportXdsQps(1, tags);
    }
    if (success) {
        _grpcAdsClient->SendACK(typeUrl, nonce);
    } else {
        _grpcAdsClient->SendNACK(typeUrl, nonce, "processXDSError");
    }
}

string syncSince(uint64_t interval) {
    if (interval < 5000000) { // 5s
        return "s5";
    } else if (interval < 20000000) { // 20 s
        return "m1";
    } else if (interval < 60000000) { // 1 min
        return "m1";
    } else if (interval < 600000000) { // 10 min
        return "m10";
    } else if (interval < 3600000000) { // 1 hour
        return "m60";
    } else if (interval < 0) {
        return "neg";
    }
    return "hours";
}

void XdsClient::metricCollector() {
    int64_t last = autil::TimeUtility::currentTime();
    int64_t interval = 3 * 1000 * 1000; // 3 s
    while (!_stop) {
        int64_t now = autil::TimeUtility::currentTime();
        if (now >= last + interval) {
            last = now;
            if (!_metricReporter) {
                AUTIL_LOG(ERROR, "Error: _metricReporter is null!");
                break;
            }
            std::map<std::string, std::string> tags = {};
            if (_workerThreads) {
                _metricReporter->reportXdsTaskQueue(
                    _workerThreads->getItemCount(), tags);
            }
            if (_grpcAdsClient) {
                int64_t now = autil::TimeUtility::currentTime();
                std::map<std::string, std::string> tags = {
                    { "channelState", _grpcAdsClient->getChannelState() },
                    { "cdsSync", syncSince(now - _CDSsyncedTimestamp.load()) },
                    { "edsSync", syncSince(now - _EDSsyncedTimestamp.load()) },
                    { "cdsPending", bool2String(_CDSRequestTimestamp != 0) },
                    { "edsPending", bool2String(_EDSRequestTimestamp != 0) }
                };
                _metricReporter->reportXdsState(1, tags);
            }
            tags = { { "xds_type", "cds" } };
            size_t subSize;
            {
                autil::ScopedLock lock(_mutex);
                subSize = _watchedResourceNames.size();
            }
            _metricReporter->reportXdsSize(_xdsStore->cdsSize(), tags);
            tags["xds_type"] = "eds";
            _metricReporter->reportXdsSize(_xdsStore->edsSize(), tags);
            tags = {};
            _metricReporter->reportXdsSubscribeSize(subSize, tags);
        }
        usleep(100 * 1000); // 100 ms
    }
}
} // namespace multi_call
