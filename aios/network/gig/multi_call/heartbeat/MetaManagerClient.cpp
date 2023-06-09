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
#include "aios/network/gig/multi_call/heartbeat/MetaManagerClient.h"
#include "aios/network/gig/multi_call/heartbeat/MetaUtil.h"
#include <unordered_set>
#include <utility>

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, MetaManagerClient);

MetaManagerClient::MetaManagerClient(const ConnectionManagerPtr &cm)
    : _connectionManager(cm) {}

MetaManagerClient::~MetaManagerClient() {}

/**
 * fill only received biz_meta (biz, version) for update.
 **/
void MetaManagerClient::fillHbRequest(const std::string &hbSpec,
                                      HeartbeatRequest &hbRequest) {
    HbMetaInfoPtr metaInfo = getMetaBySpec(hbSpec);
    if (!metaInfo) {
        return;
    }

    for (auto &bizMeta : metaInfo->meta.biz_meta()) {
        BizMeta *newBizMeta = hbRequest.add_biz_meta();
        newBizMeta->set_biz_name(bizMeta.biz_name());
        newBizMeta->set_version(bizMeta.version());
    }
}

bool MetaManagerClient::generateHbInfoIfCanDoHb(TopoNode &topoNode) {
    ProtocolType type(MC_PROTOCOL_UNKNOWN);
    Spec &spec = topoNode.spec;
    if (INVALID_PORT != spec.ports[MC_PROTOCOL_ARPC] &&
        _connectionManager->supportProtocol(MC_PROTOCOL_ARPC)) {
        type = MC_PROTOCOL_ARPC;
    } else if (INVALID_PORT != spec.ports[MC_PROTOCOL_GRPC] &&
               _connectionManager->supportProtocol(MC_PROTOCOL_GRPC)) {
        type = MC_PROTOCOL_GRPC;
    }

    if (MC_PROTOCOL_UNKNOWN != type) {
        topoNode.hbInfo.reset(new HbInfo);
        topoNode.hbInfo->heartbeatProtocol = type;
        topoNode.hbInfo->heartbeatSpec = spec.getAnetSpec(type);
        return true;
    }
    return false;
}

void MetaManagerClient::fillMetaInTopoNode(TopoNodeVec &topoNodeVec) {
    autil::ScopedReadLock lock(_metaLock);
    std::unordered_map<std::string, HbMetaInfoPtr>::iterator iter, end;
    end = _spec2meta.end();
    for (TopoNode &topoNode : topoNodeVec) {
        if (topoNode.supportHeartbeat && generateHbInfoIfCanDoHb(topoNode)) {
            HbInfoPtr &hbInfo = topoNode.hbInfo;
            iter = _spec2meta.find(hbInfo->heartbeatSpec);
            if (iter != end) {
                hbInfo->updateHbMeta(iter->second);
            }
        }
    }
}

HbMetaInfoPtr MetaManagerClient::getMetaBySpec(const std::string &spec) {
    autil::ScopedReadLock lock(_metaLock);
    auto iter = _spec2meta.find(spec);
    if (iter != _spec2meta.end()) {
        return iter->second;
    }
    return HbMetaInfoPtr();
}

bool MetaManagerClient::clearObsoleteSpec(
    const std::unordered_set<std::string> &allSpecSet) {
    {
        autil::ScopedReadLock lock(_metaLock);
        // clear obsoleted is to avoid _spec2meta inflating,
        // but if size of all spec >= size of _spec2meta (the most cases),
        // it will not inflate
        if (allSpecSet.size() >= _spec2meta.size()) {
            return false;
        }
    }

    {
        // to clear obsolete
        autil::ScopedWriteLock lock(_metaLock);
        std::unordered_map<std::string, HbMetaInfoPtr> spec2metaKeep;
        std::unordered_map<std::string, HbMetaInfoPtr>::iterator metaIter,
            metaIterEnd = _spec2meta.end();
        for (auto iter = allSpecSet.begin(); iter != allSpecSet.end(); iter++) {
            metaIter = _spec2meta.find(*iter);
            if (metaIter != metaIterEnd) {
                spec2metaKeep[*iter] =
                    std::move(metaIter->second); // just move out
            }
        }
        _spec2meta.swap(spec2metaKeep);
    }
    return true;
}

/**
 * lookup old meta with spec
 * update each provider meta if metaInfo updated
 **/
bool MetaManagerClient::updateMetaInfo(
    const std::string &spec, std::vector<SearchServiceProviderPtr> &providerVec,
    const Meta &newMeta) {

    HbMetaInfoPtr oldMetaInfo;
    {
        autil::ScopedReadLock lock(_metaLock);
        auto iter = _spec2meta.find(spec);
        if (iter != _spec2meta.end()) {
            oldMetaInfo = iter->second;

            if (!needUpdate(oldMetaInfo->meta, newMeta)) {
                for (auto &provider : providerVec) {
                    int64_t time = autil::TimeUtility::currentTime();
                    provider->updateHeartbeatTime(time);
                }
                return false;
            }
        }
    }

    HbMetaInfoPtr metaInfo = generateNewMetaInfo(oldMetaInfo, newMeta);
    {
        autil::ScopedWriteLock lock(_metaLock);
        _spec2meta[spec] = metaInfo;
    }

    // update meta to provider
    for (auto &provider : providerVec) {
        provider->updateHeartbeatInfo(metaInfo);
    }
    return true;
}

/**
 * merge oldMetaInfo with newMeta info
 * use oldMetaInfo if bizname not in newMeta
 * (replace version in oldMetaInfo with version in oldMeta for same biz)
 * return new meta
 **/
HbMetaInfoPtr
MetaManagerClient::generateNewMetaInfo(const HbMetaInfoPtr &oldMetaInfo,
                                       const Meta &newMeta) {
    HbMetaInfoPtr metaInfo(new HbMetaInfo);
    Meta &meta = metaInfo->meta;
    // meta node
    *meta.mutable_meta_node() = newMeta.meta_node();
    if (oldMetaInfo != nullptr) {
        MetaUtil::addBizMetaTo(oldMetaInfo->meta, &meta, false);
    }
    MetaUtil::addBizMetaTo(newMeta, &meta, true);
    return metaInfo;
}

/**
 * return true if content not equal
 * return false if newMeta.biz_meta is empty
 **/
bool MetaManagerClient::needUpdate(const Meta &meta, const Meta &newMeta) {
    std::string oldStr, newStr;
    // biz topos
    int size = newMeta.biz_meta_size();
    if (size > 0 && size != meta.biz_meta_size()) {
        return true;
    } else {
        for (int i = 0; i < size; i++) {
            if (!meta.biz_meta(i).SerializeToString(&oldStr) ||
                !newMeta.biz_meta(i).SerializeToString(&newStr)) { // serialize
                                                                   // failed
                return true;
            }
            if (oldStr != newStr) {
                return true;
            }
        }
    }
    // meta node
    if (meta.has_meta_node() || newMeta.has_meta_node()) {
        if (!meta.meta_node().SerializeToString(&oldStr) ||
            !newMeta.meta_node().SerializeToString(&newStr)) {
            return true;
        }
        if (oldStr != newStr) {
            return true;
        }
    }

    return false;
}

void MetaManagerClient::addSpecMeta(const std::string &spec,
                                    HbMetaInfoPtr &metaInfo) {
    autil::ScopedWriteLock lock(_metaLock);
    _spec2meta[spec] = metaInfo;
}

size_t MetaManagerClient::getSpec2MetaSize() {
    autil::ScopedReadLock lock(_metaLock);
    return _spec2meta.size();
}

} // namespace multi_call
