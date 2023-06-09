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
#ifndef ISEARCH_MULTI_CALL_RESOURCECOMPOSER_H
#define ISEARCH_MULTI_CALL_RESOURCECOMPOSER_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/config/FlowControlConfig.h"
#include "aios/network/gig/multi_call/interface/QuerySession.h"
#include "aios/network/gig/multi_call/interface/RequestGenerator.h"
#include "aios/network/gig/multi_call/metric/ReplyInfoCollector.h"
#include "aios/network/gig/multi_call/service/SearchServiceProvider.h"
#include "aios/network/gig/multi_call/service/SearchServiceReplica.h"
#include "aios/network/gig/multi_call/service/SearchServiceResource.h"
#include "aios/network/gig/multi_call/service/SearchServiceSnapshot.h"

namespace multi_call {

class ResourceComposer {
public:
    ResourceComposer(const SearchServiceSnapshotPtr &snapshot,
                     const FlowConfigSnapshotPtr &flowConfigSnapshot);
    ~ResourceComposer();

private:
    ResourceComposer(const ResourceComposer &);
    ResourceComposer &operator=(const ResourceComposer &);

public:
    uint32_t prepareResource(const QuerySessionPtr &querySession, const RequestGeneratorPtr &generator,
                             const ReplyInfoCollectorPtr &replyInfoCollector,
                             SearchServiceResourceVector &resourceVec, size_t &providerCount);
    uint32_t createTempResource(const RequestGeneratorPtr &generator,
                                const ReplyInfoCollectorPtr &replyInfoCollector,
                                SearchServiceResourceVector &searchResourceVec,
                                size_t &providerCount);

public:
    static void
    doCreateSearchServiceResource(const RequestGeneratorPtr &generator,
                                  const MatchTagMapPtr &matchTagMap, const std::string &bizName,
                                  const RequestPtr &request, PartIdTy partCount, PartIdTy partId,
                                  VersionTy version, const SearchServiceReplicaPtr &replica,
                                  const SearchServiceProviderPtr &provider, RequestType requestType,
                                  const FlowControlConfigPtr &flowControlConfig,
                                  SearchServiceResourceVector &searchResourceVec);

    static void fillUserRequestType(const RequestGeneratorPtr &generator,
                                    const PartRequestMap &requestMap);

private:
    void collectProviderCount(
        const std::string &bizName,
        const SearchServiceResourceVector &searchResourceVec, size_t beginIndex,
        const ReplyInfoCollectorPtr &replyInfoCollector, size_t &providerCount);

private:
    SearchServiceSnapshotPtr _snapshot;
    FlowConfigSnapshotPtr _flowConfigSnapshot;
private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(ResourceComposer);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_RESOURCECOMPOSER_H
