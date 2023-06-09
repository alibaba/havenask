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
#include "aios/network/gig/multi_call/service/ResourceComposer.h"
#include "aios/network/gig/multi_call/service/LoadBalancerContext.h"
#include "aios/network/gig/multi_call/service/ProviderSelector.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, ResourceComposer);

ResourceComposer::ResourceComposer(const SearchServiceSnapshotPtr &snapshot,
                                   const FlowConfigSnapshotPtr &flowConfigSnapshot)
    : _snapshot(snapshot)
    , _flowConfigSnapshot(flowConfigSnapshot)
{
}

ResourceComposer::~ResourceComposer() {}

uint32_t ResourceComposer::prepareResource(const QuerySessionPtr &querySession,
                                           const RequestGeneratorPtr &generator,
                                           const ReplyInfoCollectorPtr &replyInfoCollector,
                                           SearchServiceResourceVector &searchResourceVec,
                                           size_t &providerCount)
{
    if (!generator) {
        AUTIL_LOG(ERROR, "generator is null");
        return 0;
    }
    const auto &spec = generator->getSpec();
    if (!spec.ip.empty()) {
        return createTempResource(generator, replyInfoCollector,
                                  searchResourceVec, providerCount);
    }

    size_t originalSize = searchResourceVec.size();
    std::string bizName = generator->getBizName();
    auto lbContext = querySession->getLoadBalancerContext();

    if (!lbContext) {
        AUTIL_LOG(ERROR, "load balance context is null, bizName:[%s], request id:[%s]",
                  bizName.c_str(), generator->getRequestId().c_str());
        return 1;
    }
    auto versionSelector = lbContext->getVersionSelector(bizName);
    auto providerSelector = lbContext->getProviderSelector(bizName);
    if (!versionSelector || !providerSelector) {
        AUTIL_LOG(ERROR,
                  "create version or provider selector failed, bizName:[%s], "
                  "request id:[%s]",
                  bizName.c_str(), generator->getRequestId().c_str());
        return 1;
    }

    auto cachedRequestGenerator = std::make_shared<CachedRequestGenerator>(generator);
    bool sVersion = versionSelector->select(cachedRequestGenerator, _snapshot);
    const SearchServiceSnapshotInVersionPtr &versionSnapshot =
        versionSelector->getBizVersionSnapshot();
    if (!sVersion || !versionSnapshot) {
        replyInfoCollector->addErrorCode(
            bizName, MULTI_CALL_REPLY_ERROR_VERSION_NOT_EXIST);
        return 1;
    }
    cachedRequestGenerator->setClearCacheAfterGet(true);
    // select normal
    uint32_t expectProviderCount = providerSelector->select(
        cachedRequestGenerator, versionSnapshot, _flowConfigSnapshot, replyInfoCollector,
        false, searchResourceVec);

    // select probe
    const SearchServiceSnapshotInVersionPtr &probeVersionSnapshot =
        versionSelector->getProbeBizVersionSnapshot();
    providerSelector->select(cachedRequestGenerator, probeVersionSnapshot,
                             _flowConfigSnapshot, replyInfoCollector, true,
                             searchResourceVec);

    // select copy
    const std::vector<SearchServiceSnapshotInVersionPtr> &copyVersionSnapshots =
        versionSelector->getCopyBizVersionSnapshots();
    for (const auto &copyVersionSnapshot : copyVersionSnapshots) {
        providerSelector->select(cachedRequestGenerator, copyVersionSnapshot,
                                 _flowConfigSnapshot, replyInfoCollector, false,
                                 searchResourceVec);
    }

    collectProviderCount(bizName, searchResourceVec, originalSize,
                         replyInfoCollector, providerCount);
    return expectProviderCount;
}

uint32_t ResourceComposer::createTempResource(
    const RequestGeneratorPtr &generator,
    const ReplyInfoCollectorPtr &replyInfoCollector,
    SearchServiceResourceVector &searchResourceVec, size_t &providerCount) {
    PartIdTy partCnt = 1;
    PartIdTy partId = 0;
    VersionTy version = INVALID_VERSION_ID;
    const auto &bizName = generator->getBizName();
    SearchServiceReplicaPtr replica(
        new SearchServiceReplica(_snapshot->getMiscConfig()));
    TopoNode tmpNode;
    tmpNode.nodeId = "tmpNodeId";
    tmpNode.bizName = bizName;
    tmpNode.partCnt = partCnt;
    tmpNode.partId = partId;
    tmpNode.version = version;
    tmpNode.weight = MAX_WEIGHT;
    tmpNode.spec = generator->getSpec();
    SearchServiceProviderPtr provider(
        new SearchServiceProvider(_snapshot->getConnectionManager(), tmpNode));
    replica->addSearchServiceProvider(provider);
    PartRequestMap requestMap;
    generator->generate(partCnt, requestMap);
    if (generator->hasError()) {
        replyInfoCollector->addErrorCode(bizName, MULTI_CALL_ERROR_GENERATE);
        providerCount = 0;
        AUTIL_LOG(ERROR, "generate request failed, partCount: %d", partCnt);
        return partCnt;
    }
    fillUserRequestType(generator, requestMap);
    auto expectProviderCount = requestMap.size();
    const auto &request = requestMap[partId];
    if (!request) {
        replyInfoCollector->addErrorCode(
            bizName, MULTI_CALL_REPLY_ERROR_REQUEST_NOT_EXIST);
        providerCount = 0;
        AUTIL_LOG(ERROR, "generated request is null");
        return expectProviderCount;
    }

    const auto &strategy = generator->getFlowControlStrategy();
    const auto &flowControlConfig =
        _flowConfigSnapshot->getFlowControlConfig(strategy);

    doCreateSearchServiceResource(generator, nullptr, bizName, request, partCnt, partId, version,
                                  replica, provider, RT_NORMAL, flowControlConfig,
                                  searchResourceVec);
    providerCount = 1;
    return expectProviderCount;
}

void ResourceComposer::doCreateSearchServiceResource(
    const RequestGeneratorPtr &generator, const MatchTagMapPtr &matchTagMap, const string &bizName,
    const RequestPtr &request, PartIdTy partCount, PartIdTy partId, VersionTy version,
    const SearchServiceReplicaPtr &replica, const SearchServiceProviderPtr &provider,
    RequestType requestType, const FlowControlConfigPtr &flowControlConfig,
    SearchServiceResourceVector &searchResourceVec)
{
    if (!provider || !replica) {
        return;
    }
    SearchServiceResourcePtr searchResourcePtr(new SearchServiceResource(
        bizName, generator->getRequestId(), generator->getSourceId(), request, replica, provider, requestType));
    searchResourcePtr->setDisableRetry(generator->getDisableRetry());
    searchResourcePtr->setMatchTags(matchTagMap);
    if (!searchResourcePtr->init(partCount, partId, version, flowControlConfig)) {
        return;
    }
    searchResourceVec.push_back(searchResourcePtr);
}

void ResourceComposer::fillUserRequestType(const RequestGeneratorPtr &generator,
                                           const PartRequestMap &requestMap) {
    auto type = generator->getUserRequestType();
    for (const auto &pair : requestMap) {
        const auto &request = pair.second;
        if (request) {
            request->setUserRequestType(type);
        }
    }
}

void ResourceComposer::collectProviderCount(
    const string &bizName, const SearchServiceResourceVector &searchResourceVec,
    size_t beginIndex, const ReplyInfoCollectorPtr &replyInfoCollector,
    size_t &providerCount) {
    size_t normalProviderCount = 0;
    size_t probeProviderCount = 0;
    size_t copyProviderCount = 0;
    for (size_t i = beginIndex; i < searchResourceVec.size(); i++) {
        const auto &resource = searchResourceVec[i];
        auto requestType = resource->getRequestType();
        switch (requestType) {
        case RT_NORMAL:
            normalProviderCount++;
            break;
        case RT_PROBE:
            probeProviderCount++;
            break;
        case RT_COPY:
            copyProviderCount++;
            break;
        }
    }
    replyInfoCollector->addProbeCallNum(bizName, probeProviderCount);
    replyInfoCollector->addCopyCallNum(bizName, copyProviderCount);
    providerCount = normalProviderCount;
}

} // namespace multi_call
