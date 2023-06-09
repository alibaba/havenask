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
#ifndef ISEARCH_MULTI_CALL_PROVIDER_SELECTOR_H
#define ISEARCH_MULTI_CALL_PROVIDER_SELECTOR_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/config/MultiCallConfig.h"
#include "aios/network/gig/multi_call/service/CachedRequestGenerator.h"
#include "aios/network/gig/multi_call/service/SearchServiceProvider.h"
#include "aios/network/gig/multi_call/service/SearchServiceSnapshot.h"

namespace multi_call {

class ProviderSelector {
public:
    ProviderSelector(const std::string &bizName);
    virtual ~ProviderSelector();

private:
    ProviderSelector(const ProviderSelector &) = delete;
    ProviderSelector &operator=(const ProviderSelector &) = delete;

public:
    uint32_t select(const std::shared_ptr<CachedRequestGenerator> &generator,
                    const SearchServiceSnapshotInVersionPtr &versionSnapshot,
                    const FlowConfigSnapshotPtr &flowConfigSnapshot,
                    const ReplyInfoCollectorPtr &replyInfoCollector,
                    bool probeOnly,
                    SearchServiceResourceVector &searchResourceVec);

    SearchServiceProviderPtr selectBackupProvider(
        const SearchServiceSnapshotInVersionPtr &versionSnapshot,
        const SearchServiceProviderPtr &provider, PartIdTy partId,
        SourceIdTy sourceId, const MatchTagMapPtr &matchTagMap,
        const FlowControlConfigPtr &flowControlConfig);

private:
    void selectFromNormalVersion(
        const RequestGeneratorPtr &generator,
        const SearchServiceSnapshotInVersionPtr &versionSnapshot,
        const FlowControlConfigPtr &flowControlConfig,
        const FlowControlParam &flowControlParam, PartIdTy partId,
        const RequestPtr &request, bool probeOnly,
        SearchServiceResourceVector &searchResourceVec);
    void selectFromCopyVersion(
        const RequestGeneratorPtr &generator,
        const SearchServiceSnapshotInVersionPtr &versionSnapshot,
        PartIdTy partId, const RequestPtr &request,
        SearchServiceResourceVector &searchResourceVec);

protected:
    std::string _bizName;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(ProviderSelector);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_PROVIDER_SELECTOR_H
