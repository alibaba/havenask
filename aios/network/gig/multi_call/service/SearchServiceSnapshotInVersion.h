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
#ifndef ISEARCH_MULTI_CALL_SEARCHSERVICESNAPSHOTINVERSION_H
#define ISEARCH_MULTI_CALL_SEARCHSERVICESNAPSHOTINVERSION_H

#include "aios/network/gig/multi_call/common/VersionInfo.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/service/SearchServiceProvider.h"
#include "aios/network/gig/multi_call/service/SearchServiceReplica.h"
#include "autil/Lock.h"
#include <set>

namespace multi_call {

class SearchServiceSnapshotInVersion {
public:
    SearchServiceSnapshotInVersion(VersionTy version, PartIdTy partCnt,
                                   const MiscConfigPtr &miscConfig);
    ~SearchServiceSnapshotInVersion();

private:
    SearchServiceSnapshotInVersion(const SearchServiceSnapshotInVersion &);
    SearchServiceSnapshotInVersion &
    operator=(const SearchServiceSnapshotInVersion &);

public:
    bool addSearchServiceProvider(PartIdTy partId, const SearchServiceProviderPtr &provider,
                                  SearchServiceReplicaPtr &retReplica);
    void selectProvider(PartIdTy partId, SourceIdTy sourceId,
                        const FlowControlParam &param,
                        const MatchTagMapPtr &matchTagMap,
                        SearchServiceReplicaPtr &replica,
                        SearchServiceProviderPtr &provider,
                        SearchServiceProviderPtr &probeProvider,
                        RequestType &type);
    void selectProbeProvider(PartIdTy partId, const MatchTagMapPtr &matchTagMap,
                             SearchServiceReplicaPtr &replica, SearchServiceProviderPtr &provider);
    SearchServiceProviderPtr getBackupProvider(const SearchServiceProviderPtr &provider,
                                               PartIdTy partId, SourceIdTy sourceId,
                                               const MatchTagMapPtr &matchTagMap,
                                               const FlowControlConfigPtr &flowControlConfig);
    void getWeightInfo(int64_t currentTime, WeightTy &weight,
                       VersionInfo &info);
    void getWeightInfoByPart(WeightTy &weight, VersionInfo &info, const PartRequestMap &partRequestMap);
    bool constructConsistentHash(bool multiVersion);
    void toString(std::string &debugStr);
    bool isComplete();
    bool isCopyVersion() const { return 0 == _normalProviderCount; }
    VersionTy getVersion() const { return _version; }
    VersionTy getProtocalVersion() const {
        if (!_replicaMap.empty()) {
            return _replicaMap.begin()->second->getProtocalVersion();
        }
        return -1;
    }
    PartIdTy getPartCount() const { return _partCnt; }
    size_t getNormalProviderCount() const { return _normalProviderCount; }
    size_t getCopyProviderCount() const { return _copyProviderCount; }
    void fillVersionInfo(SnapshotBizInfo &bizInfo);
    void setBizName(const std::string &bizName) {
        _bizName = bizName;
    }
private:
    typedef std::map<PartIdTy, SearchServiceReplicaPtr> ReplicaMap;

private:
    std::string _bizName;
    ReplicaMap _replicaMap;
    VersionTy _version;
    PartIdTy _partCnt;
    mutable autil::ReadWriteLock _lock;
    WeightTy _weight;
    VersionInfo _versionInfo;
    int64_t _lastUpateTime;
    size_t _normalProviderCount;
    size_t _copyProviderCount;
    MiscConfigPtr _miscConfig;

private:
    AUTIL_LOG_DECLARE();
};
MULTI_CALL_TYPEDEF_PTR(SearchServiceSnapshotInVersion);

typedef std::map<SourceIdTy, SearchServiceSnapshotInVersionPtr>
    VersionSnapshotMap;

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_SEARCHSERVICESNAPSHOTINVERSION_H
