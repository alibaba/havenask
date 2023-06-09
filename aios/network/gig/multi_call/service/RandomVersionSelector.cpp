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
#include "aios/network/gig/multi_call/service/RandomVersionSelector.h"

using namespace std;

namespace multi_call {

AUTIL_LOG_SETUP(multi_call, RandomVersionSelector);

RandomVersionSelector::RandomVersionSelector(const std::string &bizName,
                                             const SubscribeServiceManagerPtr &subscribeManager)
    : VersionSelector(bizName)
    , _subscribeServiceManager(subscribeManager) {
}

bool RandomVersionSelector::select(const std::shared_ptr<CachedRequestGenerator> &generator,
                                   const SearchServiceSnapshotPtr &snapshot) {
    autil::ScopedLock selectLock(_mutex); // make sure only one select in
                                          // processing
    if (_versionSnapshot != nullptr) {
        return true;
    }

    auto bizSnapshot = getBizSnapshot(generator, snapshot);
    if (bizSnapshot == nullptr) {
        return false;
    }

    auto inputVersion = generator->getGenerator()->getVersion();
    auto preferVersion = generator->getGenerator()->getPreferVersion();

    selectVersionSnapshot(generator, bizSnapshot, inputVersion, preferVersion);
    if (_versionSnapshot == nullptr) {
        AUTIL_LOG(ERROR, "version [%ld] not exist, biz [%s] requestGenerator [%s]", inputVersion,
                  _bizName.c_str(), generator->toString().c_str());
        return false; // trigger lack of provider
    }

    selectCopyVersionSnapshots(bizSnapshot, inputVersion);
    selectProbeVersionSnapshot(bizSnapshot, inputVersion, preferVersion);
    return true;
}

SearchServiceSnapshotInBizPtr
RandomVersionSelector::getBizSnapshot(const std::shared_ptr<CachedRequestGenerator> &generator,
                                      const SearchServiceSnapshotPtr &snapshot) {
    SearchServiceSnapshotInBizPtr bizSnapshot = snapshot->getBizSnapshot(_bizName);
    if (!bizSnapshot) {
        if (!snapshot->disableBizNotExistLog()) {
            AUTIL_LOG(ERROR, "snapshot not exist, bizName:[%s]", _bizName.c_str());
        }
        if (generator->getGenerator()->getClusterName().empty()) {
            _subscribeServiceManager->subscribeBiz(_bizName);
        }
        return nullptr; // trigger lack of provider
    }
    // set already has query select
    bizSnapshot->setActive();
    return bizSnapshot;
}

void RandomVersionSelector::selectVersionSnapshot(
    const std::shared_ptr<CachedRequestGenerator> &generator,
    const SearchServiceSnapshotInBizPtr &bizSnapshot, VersionTy version, VersionTy preferVersion) {
    const VersionSnapshotMap &versionSnapshotMap = bizSnapshot->getVersionSnapshotMap();
    if (versionSnapshotMap.empty()) {
        AUTIL_LOG(WARN, "No available version.");
        return;
    }
    // user specify version, use user specified version
    if (INVALID_VERSION_ID != version) {
        _versionSnapshot = bizSnapshot->getVersionSnapshot(version);
        return;
    }
    // user specify prefer version, try use prefered version
    if (INVALID_VERSION_ID != preferVersion) {
        auto prefer = bizSnapshot->getVersionSnapshot(preferVersion);
        if (prefer) {
            _versionSnapshot = prefer;
            return;
        }
    }
    // only one version, no need select
    if (versionSnapshotMap.size() == 1) {
        _versionSnapshot = versionSnapshotMap.begin()->second;
        return;
    }

    _versionSnapshot = bizSnapshot->getVersionSnapshot(
        selectVersion(generator, bizSnapshot, _randomGenerator.get()));
}

VersionTy
RandomVersionSelector::selectVersion(const std::shared_ptr<CachedRequestGenerator> &generator,
                                     const SearchServiceSnapshotInBizPtr &bizSnapshot,
                                     SourceIdTy sourceId) {
    vector<pair<VersionTy, WeightTy>> completeWeightVec, notCompleteWeightVec, stopWeightVec;
    std::map<VersionTy, size_t> versionNormalProviderCountMap;
    getWeightVec(generator, bizSnapshot, completeWeightVec, notCompleteWeightVec, stopWeightVec,
                 versionNormalProviderCountMap);

    VersionTy retVersion = INVALID_VERSION_ID;
    if (!completeWeightVec.empty()) {
        retVersion = selectVersionByWeightOrProviderCount(bizSnapshot, completeWeightVec,
                                                          versionNormalProviderCountMap, sourceId);
    }
    if (retVersion == INVALID_VERSION_ID && !notCompleteWeightVec.empty()) {
        retVersion = selectVersionByWeightOrProviderCount(bizSnapshot, notCompleteWeightVec,
                                                          versionNormalProviderCountMap, sourceId);
    }
    if (retVersion == INVALID_VERSION_ID && !stopWeightVec.empty()) {
        retVersion = selectVersionByWeightOrProviderCount(bizSnapshot, stopWeightVec,
                                                          versionNormalProviderCountMap, sourceId);
    }
    return retVersion;
}

void RandomVersionSelector::getWeightVec(
    const std::shared_ptr<CachedRequestGenerator> &generator,
    const SearchServiceSnapshotInBizPtr &bizSnapshot,
    std::vector<std::pair<VersionTy, WeightTy>> &completeWeightVec,
    std::vector<std::pair<VersionTy, WeightTy>> &notCompleteWeightVec,
    std::vector<std::pair<VersionTy, WeightTy>> &stopWeightVec,
    std::map<VersionTy, size_t> &versionNormalProviderCountMap) {
    auto &versionSnapshotMap = bizSnapshot->getVersionSnapshotMap();

    auto currentTime = autil::TimeUtility::currentTime();
    for (const auto &snapshot : versionSnapshotMap) {
        auto version = snapshot.first;
        const auto &snapshotInVersion = snapshot.second;

        auto partCount = snapshotInVersion->getPartCount();
        if (partCount == 0) {
            continue;
        }

        PartRequestMap requestMap;
        generator->generate(snapshotInVersion->getPartCount(), requestMap);

        WeightTy weight = 0;
        VersionInfo info;
        if (requestMap.size() == 0 || requestMap.size() == partCount) {
            // empty or full request, could use optimized function
            snapshotInVersion->getWeightInfo(currentTime, weight, info);
        } else {
            snapshotInVersion->getWeightInfoByPart(weight, info, requestMap);
        }

        if (info.complete()) {
            completeWeightVec.push_back(make_pair(version, weight));
        } else if (info.hasStopped()) {
            // provider of one partition in this version all stoped, low
            // priority version stoped version weight should be 1, random choose
            // from all version
            stopWeightVec.push_back(make_pair(version, 1));
        } else {
            notCompleteWeightVec.push_back(make_pair(version, weight));
        }
        versionNormalProviderCountMap[version] = info.normalProviderCount;
    }
}

VersionTy RandomVersionSelector::selectVersionByWeightOrProviderCount(
    const SearchServiceSnapshotInBizPtr &bizSnapshot,
    const std::vector<pair<VersionTy, WeightTy>> &weightVec,
    const std::map<VersionTy, size_t> &versionNormalProviderCountMap, SourceIdTy sourceId) {
    WeightTy sumWeight = 0;
    for (size_t i = 0; i < weightVec.size(); ++i) {
        sumWeight += weightVec[i].second;
    }
    if (sumWeight <= 0) {
        sumWeight = 0;
        vector<pair<VersionTy, WeightTy>> normalProviderWeightVec;
        for (auto &weightPair : weightVec) {
            auto iter = versionNormalProviderCountMap.find(weightPair.first);
            if (iter != versionNormalProviderCountMap.end() && iter->second > 0) {
                sumWeight += iter->second;
                normalProviderWeightVec.push_back(make_pair(weightPair.first, iter->second));
            }
        }
        if (sumWeight <= 0) {
            AUTIL_LOG(WARN, "select version failed on normal provider and weight");
            return INVALID_VERSION_ID;
        }
        return selectVersionByWeightVec(sumWeight, normalProviderWeightVec, sourceId);
    }
    return selectVersionByWeightVec(sumWeight, weightVec, sourceId);
}

VersionTy RandomVersionSelector::selectVersionByWeightVec(
    WeightTy sumWeight, const vector<pair<VersionTy, WeightTy>> &weightVec, SourceIdTy sourceId) {
    if (sumWeight == 0) {
        AUTIL_LOG(ERROR, "bug!!! sum weight is 0");
        return INVALID_VERSION_ID;
    }
    int64_t key = sourceId % sumWeight;
    for (size_t i = 0; i < weightVec.size(); ++i) {
        key -= weightVec[i].second;
        if (key < 0) {
            return weightVec[i].first;
        }
    }
    return INVALID_VERSION_ID;
}

void RandomVersionSelector::selectCopyVersionSnapshots(
    const SearchServiceSnapshotInBizPtr &bizSnapshot, VersionTy version) {
    if (version != INVALID_VERSION_ID) {
        return;
    }

    const auto &copyVersionSnapshotMap = bizSnapshot->getCopyVersionSnapshotMap();
    auto normalProviderCount = bizSnapshot->getNormalProviderCount();
    for (const auto &copySnapshot : copyVersionSnapshotMap) {
        if (needCopy(copySnapshot.second->getCopyProviderCount(), normalProviderCount)) {
            _copyVersionSnapshots.emplace_back(copySnapshot.second);
        }
    }
}

void RandomVersionSelector::selectProbeVersionSnapshot(
    const SearchServiceSnapshotInBizPtr &bizSnapshot, VersionTy version, VersionTy preferVersion) {
    if (version != INVALID_VERSION_ID || bizSnapshot->getVersionSnapshotMap().size() == 1 ||
        _versionSnapshot == nullptr || preferVersion == _versionSnapshot->getVersion()) {
        // only one version or user specified version, not probe
        return;
    }

    auto probeVersion = selectVersionByWeightVec(bizSnapshot->getNormalProviderCount(),
                                                 bizSnapshot->getVersionNormalProviderCountVec(),
                                                 _randomGenerator.get());
    if (probeVersion == _versionSnapshot->getVersion() || probeVersion == INVALID_VERSION_ID) {
        // try to probe other valid version
        return;
    }

    auto probeVersionSnapshot = bizSnapshot->getVersionSnapshot(probeVersion);
    if (probeVersionSnapshot == nullptr) {
        return;
    }

    auto providerCount = probeVersionSnapshot->getNormalProviderCount();
    auto normalProviderCount = bizSnapshot->getNormalProviderCount();
    assert(providerCount <= normalProviderCount);
    if (needCopy(providerCount, normalProviderCount - providerCount)) {
        _probeVersionSnapshot = probeVersionSnapshot;
    }
}

} // namespace multi_call
