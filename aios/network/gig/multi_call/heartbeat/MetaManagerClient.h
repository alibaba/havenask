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
#ifndef ISEARCH_MULTI_CALL_METAMANAGERCLIENT_H
#define ISEARCH_MULTI_CALL_METAMANAGERCLIENT_H

#include "aios/network/gig/multi_call/common/HbInfo.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/service/SearchServiceProvider.h"
#include "autil/Lock.h"
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace multi_call {

/**
 * MetaManagerClient manage biz - BizMeta (including MetaKV)
 * suppose there's at most one version under same biz_name
 * bizInfo under same biz version cannot change after heartbeat call.
 **/
class MetaManagerClient {
public:
    MetaManagerClient(const ConnectionManagerPtr &cm);
    ~MetaManagerClient();

private:
    MetaManagerClient(const MetaManagerClient &);
    MetaManagerClient &operator=(const MetaManagerClient &);

public:
    void fillHbRequest(const std::string &hbSpec, HeartbeatRequest &hbRequest);
    void fillMetaInTopoNode(TopoNodeVec &topoNodeVec);
    bool updateMetaInfo(const std::string &spec,
                        std::vector<SearchServiceProviderPtr> &providerVec,
                        const Meta &newMeta);
    HbMetaInfoPtr getMetaBySpec(const std::string &spec);
    bool clearObsoleteSpec(const std::unordered_set<std::string> &allSpecSet);
    bool generateHbInfoIfCanDoHb(TopoNode &topoNode); // public for ut

private:
    HbMetaInfoPtr generateNewMetaInfo(const HbMetaInfoPtr &oldMetaInfo,
                                      const Meta &newMeta);
    bool needUpdate(const Meta &meta, const Meta &newMeta);

public: // for unit test
    void addSpecMeta(const std::string &spec, HbMetaInfoPtr &metaInfo);
    size_t getSpec2MetaSize();

private:
    const ConnectionManagerPtr _connectionManager;
    autil::ReadWriteLock _metaLock;
    std::unordered_map<std::string, HbMetaInfoPtr> _spec2meta;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(MetaManagerClient);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_METAMANAGERCLIENT_H
