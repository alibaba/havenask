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
#include "aios/network/gig/multi_call/service/ProviderSelector.h"
#include "aios/network/gig/multi_call/service/FlowControlParam.h"
#include "aios/network/gig/multi_call/service/ResourceComposer.h"
#include "autil/Log.h"
#include <string>

using namespace std;
using namespace autil;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, ProviderSelector);

ProviderSelector::ProviderSelector(const std::string &bizName)
    : _bizName(bizName) {}

ProviderSelector::~ProviderSelector() {}

void ProviderSelector::selectFromNormalVersion(
    const RequestGeneratorPtr &generator,
    const SearchServiceSnapshotInVersionPtr &versionSnapshot,
    const FlowControlConfigPtr &flowControlConfig,
    const FlowControlParam &flowControlParam, PartIdTy partId,
    const RequestPtr &request, bool probeOnly,
    SearchServiceResourceVector &searchResourceVec) {
    SearchServiceReplicaPtr replica;
    SearchServiceProviderPtr provider;
    SearchServiceProviderPtr probeProvider;
    RequestType probeType = RT_PROBE;
    SourceIdTy sourceId = generator->getSourceId();
    auto tags = generator->cloneMatchTags();
    versionSnapshot->selectProvider(partId, sourceId, flowControlParam, tags, replica, provider,
                                    probeProvider, probeType);
    auto partCnt = versionSnapshot->getPartCount();
    auto version = versionSnapshot->getVersion();
    if (!probeOnly) {
        ResourceComposer::doCreateSearchServiceResource(
            generator, tags, _bizName, request, partCnt, partId, version, replica, provider,
            RT_NORMAL, flowControlConfig, searchResourceVec);
        if (!provider || !replica) {
            AUTIL_INTERVAL_LOG(
                200, WARN,
                "no provider found for biz[%s] partId[%d], maybe "
                "degrade triggered",
                _bizName.c_str(), partId);
        }
    }
    AUTIL_LOG(DEBUG, "provider: %s, probe: %s",
              provider ? provider->getNodeId().c_str() : "",
              probeProvider ? probeProvider->getNodeId().c_str() : "");
    if ((probeOnly || provider != probeProvider) && (!generator->getDisableProbe())) {
        ResourceComposer::doCreateSearchServiceResource(
            generator, tags, _bizName, request, partCnt, partId, version, replica, probeProvider,
            probeType, flowControlConfig, searchResourceVec);
    }
}

void ProviderSelector::selectFromCopyVersion(
    const RequestGeneratorPtr &generator,
    const SearchServiceSnapshotInVersionPtr &versionSnapshot, PartIdTy partId,
    const RequestPtr &request, SearchServiceResourceVector &searchResourceVec) {
    SearchServiceReplicaPtr replica;
    SearchServiceProviderPtr provider;
    auto tags = generator->cloneMatchTags();
    versionSnapshot->selectProbeProvider(partId, tags, replica, provider);

    auto partCnt = versionSnapshot->getPartCount();
    auto version = versionSnapshot->getVersion();
    ResourceComposer::doCreateSearchServiceResource(generator, tags, _bizName, request, partCnt,
                                                    partId, version, replica, provider, RT_COPY,
                                                    FlowControlConfigPtr(), searchResourceVec);
}

uint32_t ProviderSelector::select(
    const std::shared_ptr<CachedRequestGenerator> &generator,
    const SearchServiceSnapshotInVersionPtr &versionSnapshot,
    const FlowConfigSnapshotPtr &flowConfigSnapshot,
    const ReplyInfoCollectorPtr &replyInfoCollector, bool probeOnly,
    SearchServiceResourceVector &searchResourceVec) {
    if (!versionSnapshot) {
        replyInfoCollector->addErrorCode(
            _bizName, MULTI_CALL_REPLY_ERROR_VERSION_NOT_EXIST);
        return 1;
    }

    const auto &strategy = generator->getGenerator()->getFlowControlStrategy();
    const FlowControlConfigPtr &flowControlConfig =
        flowConfigSnapshot->getFlowControlConfig(strategy);

    auto partCnt = versionSnapshot->getPartCount();
    PartRequestMap requestMap;
    generator->generate(partCnt, requestMap);
    if (generator->getGenerator()->hasError()) {
        replyInfoCollector->addErrorCode(_bizName, MULTI_CALL_ERROR_GENERATE);
        return partCnt;
    }
    ResourceComposer::fillUserRequestType(generator->getGenerator(), requestMap);

    FlowControlParam flowControlParam(flowControlConfig);
    flowControlParam.partitionCount = requestMap.size();
    flowControlParam.disableDegrade = generator->getGenerator()->getDisableDegrade();
    flowControlParam.ignoreWeightLabelInConsistentHash = generator->getGenerator()->getIgnoreWeightLabelInConsistentHash();
    auto isCopyVersion = versionSnapshot->isCopyVersion();
    size_t index = 0;
    for (auto it = requestMap.begin(); it != requestMap.end(); it++, index++) {
        PartIdTy pid = it->first;
        const auto &request = it->second;
        if (!request) {
            replyInfoCollector->addErrorCode(
                _bizName, MULTI_CALL_REPLY_ERROR_REQUEST_NOT_EXIST);
            AUTIL_LOG(ERROR, "generated request is null");
            continue;
        }
        if (!isCopyVersion) {
            flowControlParam.partitionIndex = index;
            selectFromNormalVersion(generator->getGenerator(), versionSnapshot,
                                    flowControlConfig, flowControlParam, pid,
                                    request, probeOnly, searchResourceVec);
        } else {
            selectFromCopyVersion(generator->getGenerator(), versionSnapshot, pid, request,
                                  searchResourceVec);
        }
    }
    return requestMap.size();
}

SearchServiceProviderPtr ProviderSelector::selectBackupProvider(
    const SearchServiceSnapshotInVersionPtr &versionSnapshot,
    const SearchServiceProviderPtr &provider, PartIdTy partId,
    SourceIdTy sourceId, const MatchTagMapPtr &matchTagMap,
    const FlowControlConfigPtr &flowControlConfig)
{
    return versionSnapshot->getBackupProvider(provider, partId, sourceId, matchTagMap,
                                              flowControlConfig);
}

} // namespace multi_call
