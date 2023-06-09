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
#ifndef ISEARCH_MULTI_CALL_RANDOMVERSIONSELECTOR_H
#define ISEARCH_MULTI_CALL_RANDOMVERSIONSELECTOR_H

#include "aios/network/gig/multi_call/config/MultiCallConfig.h"
#include "aios/network/gig/multi_call/service/VersionSelector.h"
#include "aios/network/gig/multi_call/subscribe/SubscribeServiceManager.h"
#include "autil/Lock.h"

namespace multi_call {

class RandomVersionSelector : public VersionSelector
{
public:
    RandomVersionSelector(const std::string &bizName,
                          const SubscribeServiceManagerPtr &subscribeManager);

public:
    bool select(const std::shared_ptr<CachedRequestGenerator> &generator,
                const SearchServiceSnapshotPtr &snapshot) override;

    const SearchServiceSnapshotInVersionPtr &getBizVersionSnapshot() override {
        return _versionSnapshot;
    }

    const SearchServiceSnapshotInVersionPtr &getProbeBizVersionSnapshot() override {
        return _probeVersionSnapshot;
    }

    const std::vector<SearchServiceSnapshotInVersionPtr> &getCopyBizVersionSnapshots() override {
        return _copyVersionSnapshots;
    }

private:
    SearchServiceSnapshotInBizPtr
    getBizSnapshot(const std::shared_ptr<CachedRequestGenerator> &generator,
                   const SearchServiceSnapshotPtr &snapshot);

    void selectVersionSnapshot(const std::shared_ptr<CachedRequestGenerator> &generator,
                               const SearchServiceSnapshotInBizPtr &bizSnapshot, VersionTy version,
                               VersionTy preferVersion);
    void selectCopyVersionSnapshots(const SearchServiceSnapshotInBizPtr &bizSnapshot,
                                    VersionTy version);
    void selectProbeVersionSnapshot(const SearchServiceSnapshotInBizPtr &bizSnapshot,
                                    VersionTy version, VersionTy preferVersion);

    VersionTy selectVersion(const std::shared_ptr<CachedRequestGenerator> &generator,
                            const SearchServiceSnapshotInBizPtr &bizSnapshot, SourceIdTy sourceId);
    void getWeightVec(const std::shared_ptr<CachedRequestGenerator> &generator,
                      const SearchServiceSnapshotInBizPtr &bizSnapshot,
                      std::vector<std::pair<VersionTy, WeightTy>> &completeWeightVec,
                      std::vector<std::pair<VersionTy, WeightTy>> &notCompleteWeightVec,
                      std::vector<std::pair<VersionTy, WeightTy>> &stopWeightVec,
                      std::map<VersionTy, size_t> &verisonNormalProviderCountMap);
    VersionTy selectVersionByWeightOrProviderCount(
        const SearchServiceSnapshotInBizPtr &bizSnapshot,
        const std::vector<std::pair<VersionTy, WeightTy>> &weightVec,
        const std::map<VersionTy, size_t> &versionNormalProviderCountMap, SourceIdTy sourceId);
    VersionTy selectVersionByWeightVec(WeightTy sumWeight,
                                       const std::vector<std::pair<VersionTy, WeightTy>> &weightVec,
                                       SourceIdTy sourceId);

private:
    SubscribeServiceManagerPtr _subscribeServiceManager;
    autil::ThreadMutex _mutex;
    SearchServiceSnapshotInVersionPtr _versionSnapshot;
    SearchServiceSnapshotInVersionPtr _probeVersionSnapshot;
    std::vector<SearchServiceSnapshotInVersionPtr> _copyVersionSnapshots;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(RandomVersionSelector);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_RANDOMVERSIONSELECTOR_H
