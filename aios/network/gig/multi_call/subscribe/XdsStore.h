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
#ifndef MULTI_CALL_XDSCLIENT_XDSSTORE_H
#define MULTI_CALL_XDSCLIENT_XDSSTORE_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/config/MultiCallConfig.h"
#include "aios/network/gig/multi_call/subscribe/SubscribeService.h"
#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/envoy/api/v2/cds.grpc.pb.h"
#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/envoy/api/v2/eds.grpc.pb.h"
#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/envoy/service/discovery/v2/ads.grpc.pb.h"
#include "autil/Thread.h"

namespace multi_call {

// XdsStore provides static data access for XDS, except for a thread
// periodically dump cache. This cache is only read if xdsClient cannot connect
// to server at startup.
// Usage: refer to IstioSubscribeService
class XdsStore {
public:
    static const std::string kResourcePrefix;

    XdsStore(const IstioConfig &config, const IstioMetricReporterPtr reporter);
    ~XdsStore();
    XdsStore(const XdsStore &) = delete;
    XdsStore &operator=(const XdsStore &) = delete;

    bool init();
    bool stop();

    bool clusterInfoNeedUpdate();

    // Caller should already checked if the xdsClient has the full data before
    // call this function.
    // The output should be *append* to topoNodeVecToAppend
    bool getClusterInfoMap(TopoNodeVec *topoNodeVecToAppend);

    bool loadCache();

    // may be synced, or loaded from cache
    bool isServeable();

    bool updateCDS(bool isFull,
                   std::shared_ptr<envoy::api::v2::DiscoveryResponse> resp);

    bool updateEDS(bool isFull,
                   std::shared_ptr<envoy::api::v2::DiscoveryResponse> resp);

    // delete the in-memory data for a cluster via its hostname (not resource
    // name).  This is used for deleting a subscribed cluster.
    bool clear(const std::string &hostname);

    static std::set<std::string>
    hostnamesToResources(const std::set<std::string> &clusters);
    static std::set<std::string>
    hostnamesToResources(const std::vector<std::string> &clusters);
    //  outbound|0||bizname.clustername => bizname.clustername, if prefix not
    //  exist do nothing
    static std::string resourceToHostname(const std::string &resource);
    // bizname.clustername => outbound|0||bizname.clustername
    static std::string hostnameToResource(const std::string &hostname);

    size_t cdsSize();
    size_t edsSize();

private:
    // key-value pair for a cluster in CDS
    typedef std::map<std::string, std::string> ClusterInfo;
    typedef std::map<std::string, std::shared_ptr<TopoNode> > TopoNodes;
    static const std::string kClusterInfoKey;
    static const std::string kDelKey;

    static const std::string kKeyPartNum;
    static const std::string kKeyNodeStatus;
    static const std::string kKeyOnlineStatus;
    static const std::string kKeyPartID;
    static const std::string kKeyWeight;
    static const std::string kKeyGrpcPort;
    static const std::string kKeySupportHearbeat;
    static const std::string kKeyProtocol;
    static const std::string kKeyVersion;
    static const std::string kKeyCmnodeMetaInfo;

    template <typename T>
    static T extractValue(const std::map<std::string, std::string> &kv,
                          const std::string &key, const T &defaultValue);

    static void fillTopoNode(const std::string &actBizName,
                             const envoy::api::v2::endpoint::LbEndpoint &lbe,
                             std::shared_ptr<TopoNode> topoNode);

    void cacheWriter();
    bool doWriteCache(
        autil::ThreadMutex *lock,
        const std::map<std::string, std::shared_ptr< ::google::protobuf::Any> >
            &cache,
        const std::string &type);
    // This only called xdsclient init, so no lock needed.
    // type: "cds" or "eds"
    bool doReadCache(const std::string &type);

    volatile bool _stop;
    volatile bool _cacheChanged;
    IstioConfig _config;
    autil::ThreadMutex _cdsMutex;
    // CDS
    std::map<std::string, ClusterInfo> _clusterInfoMap;
    std::map<std::string, std::shared_ptr< ::google::protobuf::Any> >
        _cdsHostnameResourceCache;
    autil::ThreadMutex _edsMutex;
    // EDS
    std::map<std::string, TopoNodes> _topoNodesMap;
    std::map<std::string, std::shared_ptr< ::google::protobuf::Any> >
        _edsHostnameResourceCache;
    // TODO (guanming.fh) is it possible not creating new EDS objects when only
    // status changes?

    IstioMetricReporterPtr _metricReporter;
    std::atomic<uint64_t> _lastUpdateTimestamp;
    std::atomic<uint64_t> _lastQueryTimestamp;
    autil::ThreadPtr _cacheThread;

    static int _rdmaPortDiff;

    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(XdsStore);
} // namespace multi_call

#endif // MULTI_CALL_XDSCLIENT_XDSSTORE_H
