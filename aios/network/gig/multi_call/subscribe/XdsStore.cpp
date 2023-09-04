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
#include "aios/network/gig/multi_call/subscribe/XdsStore.h"

#include <unistd.h>

#include "autil/EnvUtil.h"
#include "autil/legacy/base64.h"
#include "fslib/util/FileUtil.h"

using namespace std;
using namespace autil;
using namespace fslib::util;
using envoy::api::v2::Cluster;
using envoy::api::v2::ClusterLoadAssignment;
using envoy::api::v2::DiscoveryResponse;
using envoy::api::v2::endpoint::LbEndpoint;
using ::google::protobuf::Any;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, XdsStore);

const string XdsStore::kClusterInfoKey = "extension_info";
const string XdsStore::kDelKey = "deletedCluster";
const string XdsStore::kResourcePrefix = "outbound|0||";

const string XdsStore::kKeyPartNum = "partNum";
const string XdsStore::kKeyNodeStatus = "cm2NodeStatus";
const string XdsStore::kKeyOnlineStatus = "cm2OnlineStatus";
const string XdsStore::kKeyPartID = "partID";
const string XdsStore::kKeyWeight = "weight";
const string XdsStore::kKeyGrpcPort = "grpcPort";
const string XdsStore::kKeySupportHearbeat = "supportHeartbeat";
const string XdsStore::kKeyProtocol = "protocol";
const string XdsStore::kKeyVersion = "version";
const string XdsStore::kKeyCmnodeMetaInfo = "cmnodeMetaInfoBase64";

int XdsStore::_rdmaPortDiff = 0;

XdsStore::XdsStore(const IstioConfig &config, const IstioMetricReporterPtr reporter)
    : _config(config)
    , _metricReporter(reporter) {
    _lastUpdateTimestamp = 0;
    _lastQueryTimestamp = 0;
    _stop = false;
    _cacheChanged = false;
    _rdmaPortDiff = autil::EnvUtil::getEnv(GIG_RDMA_PORT_DIFF_KEY, _rdmaPortDiff);
}

XdsStore::~XdsStore() {
}

bool XdsStore::init() {
    if (_config.writeCache) {
        _cacheThread =
            autil::Thread::createThread(std::bind(&XdsStore::cacheWriter, this), "writeCache");
        if (!_cacheThread) {
            AUTIL_LOG(ERROR, "Failed create cache writing thread.");
            return false;
        }
    }
    return true;
}

bool XdsStore::stop() {
    _stop = true;
    if (_cacheThread) {
        _cacheThread->join();
        _cacheThread.reset();
    }
    return true;
}

std::string XdsStore::resourceToHostname(const std::string &resource) {
    if (autil::StringUtil::startsWith(resource, kResourcePrefix)) {
        return resource.substr(kResourcePrefix.size());
    }
    return resource;
}

std::string XdsStore::hostnameToResource(const std::string &hostname) {
    return kResourcePrefix + hostname;
}

set<std::string> XdsStore::hostnamesToResources(const set<string> &clusters) {
    set<std::string> ret;
    for (const string &cls : clusters) {
        ret.insert(hostnameToResource(cls));
    }
    return ret;
}

set<std::string> XdsStore::hostnamesToResources(const vector<string> &clusters) {
    set<std::string> ret;
    for (const string &cls : clusters) {
        ret.insert(hostnameToResource(cls));
    }
    return ret;
}

size_t XdsStore::cdsSize() {
    autil::ScopedLock lock(_cdsMutex);
    return _clusterInfoMap.size();
}

size_t XdsStore::edsSize() {
    autil::ScopedLock lock(_edsMutex);
    return _topoNodesMap.size();
}

bool XdsStore::clear(const string &hostname) {
    {
        autil::ScopedLock lock(_cdsMutex);
        _clusterInfoMap.erase(hostname);
        _cdsHostnameResourceCache.erase(hostname);
    }
    {
        autil::ScopedLock lock(_edsMutex);
        _topoNodesMap.erase(hostname);
        _edsHostnameResourceCache.erase(hostname);
    }
    return true;
}

bool XdsStore::updateCDS(bool isFull, std::shared_ptr<envoy::api::v2::DiscoveryResponse> resp) {
    const auto &resources = resp->resources();
    std::map<std::string, ClusterInfo> toAdd;
    std::map<std::string, std::shared_ptr<Any>> toCache;
    std::set<std::string> toDel;
    ClusterInfo tmpCluster;
    string hostname;
    std::map<std::string, std::string> tags = {{"xdsType", "cds"}};

    for (const auto &resource : resources) {
        Cluster cluster;
        resource.UnpackTo(&cluster);
        tmpCluster.clear();
        hostname = resourceToHostname(cluster.name());
        auto &extensionMap = cluster.extension_protocol_options();
        if (extensionMap.find(kDelKey) != extensionMap.end()) {
            toDel.insert(hostname);
            tags["op_type"] = "del";
            if (_metricReporter)
                _metricReporter->reportXdsResourceProcessed(1, tags);
            continue;
        }

        auto extensionInfo = extensionMap.find(kClusterInfoKey);
        if (extensionInfo != extensionMap.end()) {
            for (auto &extIter : extensionInfo->second.fields()) {
                tmpCluster[extIter.first] = extIter.second.string_value();
            }
        }
        toAdd[hostname] = std::move(tmpCluster);
        toCache[hostname] = make_shared<Any>(std::move(resource));
        if (isFull) {
            tags["op_type"] = "full";
        } else {
            tags["op_type"] = "inc";
        }
        if (_metricReporter)
            _metricReporter->reportXdsResourceProcessed(1, tags);
    }
    _lastUpdateTimestamp = autil::TimeUtility::currentTime();
    // Note(guanming.fh): this is dangerous and should be handled carefully. So
    // we DONOT delete the EDS of which cluster not in full CDS, because we
    // assume that full CDS is companied with full EDS, the EDS info should be
    // deleted in processing full EDS.

    if (isFull) {
        autil::ScopedLock lock(_cdsMutex);
        _clusterInfoMap.swap(toAdd);
        _cdsHostnameResourceCache.swap(toCache);
        _cacheChanged = true;
        return true;
    }
    {
        autil::ScopedLock lock(_cdsMutex);
        for (auto &add : toAdd) {
            _clusterInfoMap[add.first] = std::move(add.second);
            _cdsHostnameResourceCache[add.first] = toCache[add.first];
            _cacheChanged = true;
        }
        for (auto &del : toDel) {
            _clusterInfoMap.erase(del);
            _cdsHostnameResourceCache.erase(del);
            _cacheChanged = true;
        }
    }
    {
        autil::ScopedLock lock(_edsMutex);
        for (auto &del : toDel) {
            AUTIL_LOG(INFO, "Del bizcluster [%s] in EDS due to CDS response.", del.c_str());
            _topoNodesMap.erase(del);
            _edsHostnameResourceCache.erase(del);
            _cacheChanged = true;
        }
    }
    return true;
}

bool XdsStore::isServeable() {
    return _lastUpdateTimestamp.load() != 0;
}

template <typename T>
T XdsStore::extractValue(const std::map<std::string, std::string> &kv, const std::string &key,
                         const T &defaultValue) {
    T value;
    auto iter = kv.find(key);
    if (iter != kv.end() && autil::StringUtil::fromString(iter->second, value)) {
        return value;
    }
    return defaultValue;
}

void XdsStore::fillTopoNode(const std::string &hostname, const LbEndpoint &lbe,
                            shared_ptr<TopoNode> topoNode) {
    const string &ip = lbe.endpoint().address().socket_address().address();
    auto port = lbe.endpoint().address().socket_address().port_value();
    topoNode->bizName = hostname;
    topoNode->clusterName = hostname;
    topoNode->spec.ip = ip;
    topoNode->ssType = ST_ISTIO;

    std::map<std::string, std::string> metadata;
    for (auto &meta : lbe.metadata().filter_metadata()) {
        for (auto &field : meta.second.fields()) {
            metadata[field.first] = field.second.string_value();
        }
    }
    topoNode->partCnt = extractValue(metadata, kKeyPartNum, 1);
    topoNode->partId = extractValue(metadata, kKeyPartID, 0);
    topoNode->version = extractValue(metadata, kKeyVersion, DEFAULT_VERSION_ID);
    topoNode->weight = extractValue(metadata, kKeyWeight, MAX_WEIGHT);
    topoNode->supportHeartbeat = extractValue(metadata, kKeySupportHearbeat, false);
    int32_t grpcPort = extractValue(metadata, kKeyGrpcPort, INVALID_PORT);
    topoNode->spec.ports[MC_PROTOCOL_GRPC] = grpcPort;
    std::string protocol = extractValue(metadata, kKeyProtocol, std::string());
    if (protocol == "PT_TCP") {
        topoNode->spec.ports[MC_PROTOCOL_TCP] = port;
        topoNode->spec.ports[MC_PROTOCOL_ARPC] = port;
        if (_rdmaPortDiff != 0) {
            topoNode->spec.ports[MC_PROTOCOL_RDMA_ARPC] = port + _rdmaPortDiff;
        }
    } else if (protocol == "PT_HTTP") {
        topoNode->spec.ports[MC_PROTOCOL_HTTP] = port;
    } else {
        topoNode->spec.ports[MC_PROTOCOL_TCP] = port;
    }
    if (topoNode->partCnt < 1) {
        topoNode->partCnt = 1;
    }

    std::string ns = extractValue(metadata, kKeyNodeStatus, std::string());
    std::string os = extractValue(metadata, kKeyOnlineStatus, std::string());

    if (ns.empty() || os.empty() || (os == "OS_ONLINE" && ns == "NS_NORMAL")) {
        topoNode->isValid = true;
    } else {
        topoNode->isValid = false;
    }
    if (!topoNode->isValid) {
        AUTIL_LOG(DEBUG, "topoNode is inValid hostname [%s] ip [%s]", hostname.c_str(), ip.c_str());
    }
    string metainfoBase64 = extractValue(metadata, kKeyCmnodeMetaInfo, std::string());
    if (!metainfoBase64.empty()) {
        string metainfoStr = autil::legacy::Base64DecodeFast(metainfoBase64);
        cm_basic::NodeMetaInfo metaInfo;
        if (!metaInfo.ParseFromString(metainfoStr)) {
            AUTIL_LOG(ERROR, "Error on parse cmnode metainfo parse from string [%s].",
                      metainfoStr.c_str());
        }
        topoNode->fillNodeMeta(metaInfo);
    }
}

bool XdsStore::updateEDS(bool isFull, std::shared_ptr<envoy::api::v2::DiscoveryResponse> resp) {
    const auto &resources = resp->resources();
    std::map<std::string, TopoNodes> toAdd;
    std::map<std::string, std::shared_ptr<Any>> toCache;
    TopoNodes tmpNodes;
    string hostname;
    kmonitor::MetricsTagsPtr kmTagsFull;
    kmonitor::MetricsTagsPtr kmTagsInc;
    if (_metricReporter) {
        kmTagsFull = _metricReporter->GetMetricsTags({{"xdsType", "eds"}, {"op_type", "full"}});
        kmTagsInc = _metricReporter->GetMetricsTags({{"xdsType", "eds"}, {"op_type", "inc"}});
    }
    auto tags = kmTagsFull;
    for (const auto &resource : resources) {
        ClusterLoadAssignment cla;
        resource.UnpackTo(&cla);
        tmpNodes.clear();
        hostname = resourceToHostname(cla.cluster_name());
        for (const auto &endpoint : cla.endpoints()) {
            for (auto &lbe : endpoint.lb_endpoints()) {
                const string &ip = lbe.endpoint().address().socket_address().address();
                if (ip.empty()) {
                    AUTIL_LOG(WARN,
                              "Error endpoint with empty IP, hostname [%s], "
                              "lb_endpoints [%s]",
                              hostname.c_str(), lbe.DebugString().c_str());
                    continue;
                }
                string key = hostname + ":" + ip;
                if (!tmpNodes[key]) {
                    tmpNodes[key] = make_shared<TopoNode>();
                }
                fillTopoNode(hostname, lbe, tmpNodes[key]);
                if (!tmpNodes[key]->generateNodeId()) {
                    tmpNodes.erase(key);
                    AUTIL_LOG(ERROR, "Failed topoNode.generateNodeId(), key [%s]", key.c_str());
                }
            }
        }
        toAdd[hostname] = std::move(tmpNodes);
        toCache[hostname] = make_shared<Any>(std::move(resource));
        if (isFull) {
            tags = kmTagsFull;
            ;
        } else {
            tags = kmTagsInc;
            ;
        }
        if (_metricReporter)
            _metricReporter->reportXdsResourceProcessed(1, tags);
    }
    _lastUpdateTimestamp = autil::TimeUtility::currentTime();
    if (isFull) {
        autil::ScopedLock lock(_edsMutex);
        _topoNodesMap.swap(toAdd);
        _edsHostnameResourceCache.swap(toCache);
        _cacheChanged = true;
        return true;
    }
    {
        autil::ScopedLock lock(_edsMutex);
        for (auto &add : toAdd) {
            _topoNodesMap[add.first] = std::move(add.second);
            _edsHostnameResourceCache[add.first] = toCache[add.first];
            _cacheChanged = true;
        }
    }
    return true;
}

// The output should be *append* to topoNodeVecToAppend
bool XdsStore::getClusterInfoMap(TopoNodeVec *topoNodeVecToAppend) {
    if (topoNodeVecToAppend == NULL) {
        AUTIL_LOG(ERROR, "Error getClusterInfoMap passed a null pointer.");
        return false;
    }
    std::map<std::string, TopoNodes> topoNodesMap;
    {
        autil::ScopedLock lock(_edsMutex);
        // Assume the _topoNodesMap store pointers and all the nodes are
        // immutable, so we can first copy the pointers and release the lock.
        topoNodesMap = _topoNodesMap;
        _lastQueryTimestamp = _lastUpdateTimestamp.load();
    }
    for (const auto &ns : topoNodesMap) {
        for (const auto &n : ns.second) {
            topoNodeVecToAppend->push_back(*(n.second));
        }
    }
    return true;
}

bool XdsStore::clusterInfoNeedUpdate() {
    return _lastUpdateTimestamp.load() != _lastQueryTimestamp.load();
};

void XdsStore::cacheWriter() {
    int64_t lastWrite = autil::TimeUtility::currentTime();
    int64_t interval = _config.writeInterval * 1000 * 1000;
    int64_t now = autil::TimeUtility::currentTime();
    while (!_stop) {
        now = autil::TimeUtility::currentTime();
        if (now >= lastWrite + interval && _cacheChanged) {
            if (doWriteCache(&_cdsMutex, _cdsHostnameResourceCache, "cds") &&
                doWriteCache(&_edsMutex, _edsHostnameResourceCache, "eds")) {
                lastWrite = now;
                _cacheChanged = false;
            }
        }
        usleep(1000 * 1000); // 1000 ms
    }
};

// we build the resource to a envoy::api::v2::DiscoveryResponse>
bool XdsStore::doWriteCache(
    autil::ThreadMutex *mutex,
    const std::map<std::string, std::shared_ptr<::google::protobuf::Any>> &cache,
    const string &type) {
    std::map<std::string, std::shared_ptr<::google::protobuf::Any>> cacheCopy;
    // TODO add metric
    {
        autil::ScopedLock lock(*mutex);
        cacheCopy = cache;
    }

    envoy::api::v2::DiscoveryResponse Rsp;
    for (auto &c : cacheCopy) {
        Rsp.add_resources()->CopyFrom(*c.second);
    }

    if (cacheCopy.empty()) {
        AUTIL_LOG(WARN, "cache is empty");
    } else {
        string filename = _config.cacheFile + "." + type;
        if (FileUtil::isExist(filename)) {
            // overwrite if target file exists
            FileUtil::renameFile(filename, filename + std::string(".bak"));
        }
        string content;
        Rsp.SerializeToString(&content);
        std::map<std::string, std::string> tags = {};
        if (_metricReporter)
            _metricReporter->reportXdsCacheSize(content.size(), tags);
        if (!FileUtil::writeFile(filename, content)) {
            AUTIL_LOG(ERROR, "write cache file <%s> failed", filename.c_str());
            return false;
        }
    }
    return true;
};

bool XdsStore::doReadCache(const std::string &type) {
    string fileName = _config.cacheFile + "." + type;
    std::string content = FileUtil::readFile(fileName);
    if (content.empty()) {
        AUTIL_LOG(WARN, "cache file [%s] is empty ", fileName.c_str());
        return false;
    }
    auto rsp = make_shared<envoy::api::v2::DiscoveryResponse>();
    if (!rsp->ParseFromString(content)) {
        AUTIL_LOG(ERROR, "Failed to parse cache to pb [%s].", fileName.c_str());
        return false;
    }
    if (type == "cds") {
        return updateCDS(true, rsp);
    } else if (type == "eds") {
        return updateEDS(true, rsp);
    }
    AUTIL_LOG(ERROR, "Read cache tpye [%s] invalide.", type.c_str());
    return false;
}

bool XdsStore::loadCache() {
    return doReadCache("cds") && doReadCache("eds");
};

} // namespace multi_call
