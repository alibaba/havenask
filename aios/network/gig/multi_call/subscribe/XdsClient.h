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
#ifndef MULTI_CALL_XDSCLIENT_XDSCLIENT_H
#define MULTI_CALL_XDSCLIENT_XDSCLIENT_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/config/MultiCallConfig.h"
#include "aios/network/gig/multi_call/subscribe/GrpcAds.h"
#include "aios/network/gig/multi_call/subscribe/SubscribeService.h"
#include "aios/network/gig/multi_call/subscribe/XdsStore.h"
#include "autil/Lock.h"
#include "autil/SynchronizedQueue.h"
#include "autil/Thread.h"
#include "autil/ThreadPool.h"
// TODO Hua rename grpcads
#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/envoy/api/v2/cds.grpc.pb.h"
#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/envoy/api/v2/eds.grpc.pb.h"
#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/envoy/service/discovery/v2/ads.grpc.pb.h"

namespace multi_call {

// IstioVersionInfo struct from Pilot Xds discovery response.
struct IstioVersionInfo {
    bool full;
    std::string timestamp;
    uint64_t serverVersion;
};

// XdsClient provides subscribe from XdsServer (istio/pilot).
// XdsClient is given a set of cluster hostnames and maintians the watched
// resource names. The subscrition will first receives a Full data for EDS and
// CDS seperately, then received incremental data Or full data.
////  Note: resource
// names (outbound|0||bizname.clustername) vs hostnames (bizname.clustername)
// Usage: refer to IstioSubscribeService
class XdsClient
{
public:
    XdsClient(const IstioConfig &config, const IstioMetricReporterPtr reporter,
              std::shared_ptr<XdsStore> xdsStore);
    virtual ~XdsClient();
    XdsClient(const XdsClient &) = delete;
    XdsClient &operator=(const XdsClient &) = delete;

    bool init();

    bool addSubscribe(const std::vector<std::string> &names);
    bool deleteSubscribe(const std::vector<std::string> &names);
    bool checkSubscribeWork();
    // startSubscribe in init
    bool stopSubscribe();

    // is this client received full response (maybe futher tightly synced)
    bool isSynced();

private:
    static const std::string kCDSUrl;
    static const std::string kEDSUrl;

    // return false if parse failed, otherwise return in version info.
    static bool parseVersionInfo(const std::string &str, IstioVersionInfo *versioninfo);

    void receiveThreadFunc();
    void asyncSubThreadFunc();
    // TODO (guanming.fh) if is the first full response not use the thread pool,
    // to know if client is synced
    void processXdsResponse(std::shared_ptr<envoy::api::v2::DiscoveryResponse> resp);
    // sanity check: (1) full before all incremental msg (2) we only log error
    // on server version violation, because currently there is no mechanism to
    // maintain monotone increasing version.
    // return false, if this reponse should be rejected.
    bool xdsResponseSanityCheck(const IstioVersionInfo &vinfo, const std::string &typeUrl,
                                const std::string &nouce);
    // send SendDiscoveryRequest of EDS and CDS of watched resource
    // when *async* is false, the request is sent immediately, otherwise
    // (*async* == true), the request is sent by an interval.
    virtual bool reWatch(bool async = true);
    void metricCollector();

    volatile bool _stop;
    IstioConfig _config;
    std::set<std::string> _watchedResourceNames;
    bool _needSendSubRequest = true;
    autil::ThreadMutex _mutex;
    GrpcAdsClientPtr _grpcAdsClient;
    std::atomic<uint64_t> _CDSsyncedTimestamp;
    std::atomic<uint64_t> _EDSsyncedTimestamp;
    // ts that send request, if a full response is received this ts will be
    // reset to 0. This TS also can be used to know if a pending rsp not
    // returned.
    std::atomic<uint64_t> _CDSRequestTimestamp;
    std::atomic<uint64_t> _EDSRequestTimestamp;

    // the pre-key version is not meaningful, because the version is
    // message-based. An newer message may contains a old version key, which
    // conflicts. std::map<std::string, std::string> _clusterTypeVersionMap;
    // std::map<std::string, std::string> _endpointTypeVersionMap;

    IstioMetricReporterPtr _metricReporter;
    autil::ThreadPtr _receiveThread;
    autil::ThreadPtr _subscribeThread;
    autil::ThreadPtr _metricCollectorThread;
    autil::ThreadPoolPtr _workerThreads;
    std::shared_ptr<XdsStore> _xdsStore;

    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(XdsClient);
} // namespace multi_call

#endif // MULTI_CALL_XDSCLIENT_XDSCLIENT_H
