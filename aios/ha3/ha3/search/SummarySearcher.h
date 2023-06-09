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
#pragma once

#include "ha3/isearch.h"
#include "ha3/common/Request.h"
#include "ha3/common/Result.h"
#include "ha3/proto/BasicDefs.pb.h"
#include "ha3/common/DocIdClause.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/search/SummaryFetcher.h"
#include "ha3/summary/SummaryExtractorChain.h"
#include "ha3/summary/SummaryProfileManager.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace search {

class SummarySearcher
{
public:
    SummarySearcher(SearchCommonResource& resource,
                    const IndexPartitionReaderWrapperPtr &indexPartitionReaderWrapper,
                    indexlib::partition::PartitionReaderSnapshot *snapshot,
                    const summary::SummaryProfileManagerPtr& summaryProfileManager,
                    const config::HitSummarySchemaPtr &hitSummarySchema);
    ~SummarySearcher();
private:
    SummarySearcher(const SummarySearcher &);
    SummarySearcher& operator=(const SummarySearcher &);
public:
    common::ResultPtr search(const common::Request *request,
                             const proto::Range &hashIdRange,
                             FullIndexVersion fullIndexVersion);

    common::ResultPtr extraSearch(const common::Request *request,
                                  common::ResultPtr inputResult,
                                  const std::string& tableName);

private:
    bool doSearch(const common::GlobalIdVector &gids,
                  const common::Request *request,
                  common::Result* result);
    void doSearch(const common::GlobalIdVector &gids,
                  bool allowLackOfSummary,
                  int64_t signature,
                  autil::mem_pool::Pool *pool,
                  common::Result* result,
                  summary::SummaryExtractorChain *summaryExtractorChain,
                  summary::SummaryExtractorProvider *summaryExractorProvider);

private:
    void fetchSummaryByDocId(
            common::Result *result,
            const common::Request *request,
            const proto::Range &hashIdRange,
            FullIndexVersion fullIndexVersion);
    void fetchSummaryByPk(
            common::Result *result,
            const common::Request *request,
            const proto::Range &hashIdRange);
    bool createIndexPartReaderWrapper(FullIndexVersion fullIndexVersion);
    bool createIndexPartReaderWrapperForExtra(const std::string& tableName);
    const summary::SummaryProfile *getSummaryProfile(
            const common::Request *request,
            summary::SummaryProfileManagerPtr summaryProfileManager) ;
    bool convertPK2DocId(common::GlobalIdVector &gids,
                         const IndexPartitionReaderWrapperPtr& indexPartReaderWrapper,
                         bool ignoreDelete);
    void fillHits(common::Hits *hits,
                  autil::mem_pool::Pool *pool,
                  summary::SummaryExtractorChain *summaryExtractorChain,
                  summary::SummaryExtractorProvider *summaryExractorProvider,
                  bool allowLackOfSummary);
    void fetchSummaryDocuments(common::Hits *hits,
                               autil::mem_pool::Pool *pool,
                               summary::SummaryExtractorProvider *summaryExractorProvider,
                               bool allowLackOfSummary);
    inline SummaryFetcherPtr createSummaryFetcher();
    void doFetchSummaryDocument(SummaryFetcher &summaryFetcher,
                                common::Hits *hits,
                                autil::mem_pool::Pool *pool,
                                bool allowLackOfSummary);
    inline void resetGids(common::Result *result,
                          const common::GlobalIdVector &originalGids);
    bool declareAttributeFieldsToSummary(summary::SummaryExtractorProvider *summaryExtractorProvider);
    void genSummaryGroupIdVec(common::ConfigClause *configClause);
    // for test
    const SummaryGroupIdVec &getSummaryGroupIdVec() const { return _summaryGroupIdVec; }
    common::ResultPtr doExtraSearch(const common::Request *request,
                                    const common::ResultPtr &inputResult);

private:
    search::SearchCommonResource& _resource;
    IndexPartitionReaderWrapperPtr _indexPartitionReaderWrapper;
    indexlib::partition::PartitionReaderSnapshot *_partitionReaderSnapshot = nullptr;
    const summary::SummaryProfileManagerPtr& _summaryProfileManager;
    config::HitSummarySchemaPtr _hitSummarySchema;
    common::Tracer::TraceMode _traceMode;
    int32_t _traceLevel;
    SummaryGroupIdVec _summaryGroupIdVec;
    SummarySearchType _summarySearchType;
private:
    friend class SummarySearcherTest;
    // for test
    suez::turing::AttributeExpressionCreator *_attributeExpressionCreator;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SummarySearcher> SummarySearcherPtr;

} // namespace search
} // namespace isearch
