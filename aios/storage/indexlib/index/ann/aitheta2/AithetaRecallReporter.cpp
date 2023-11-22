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
#include "indexlib/index/ann/aitheta2/AithetaRecallReporter.h"

#include "indexlib/index/ann/aitheta2/AithetaIndexReader.h"
#include "indexlib/index/ann/aitheta2/util/ResultHolder.h"

using namespace std;
using namespace autil;

namespace indexlibv2::index::ann {

AUTIL_LOG_SETUP(indexlib.index, AithetaRecallReporter);
Status AithetaRecallReporter::Init()
{
    if (!_aithetaConfig.recallConfig.enable) {
        RETURN_STATUS_ERROR(InvalidArgs, "recall report disabled");
    }
    float sampleRatio = _aithetaConfig.recallConfig.sampleRatio;
    if (sampleRatio < 0.0f || sampleRatio > 1.0f) {
        RETURN_STATUS_ERROR(InvalidArgs, "invalid sampleRatio");
    }
    _frequency = 1 / sampleRatio;
    _threadPool.reset(new autil::ThreadPool(AithetaRecallReporter::RECALL_THREAD_NUMBER,
                                            AithetaRecallReporter::RECALL_THREAD_QUEUE_SIZE));
    if (!_threadPool->start("aitheta_recall")) {
        RETURN_STATUS_ERROR(InternalError, "start thread pool failed, threadNum[%u], queueSize[%u]",
                            AithetaRecallReporter::RECALL_THREAD_NUMBER,
                            AithetaRecallReporter::RECALL_THREAD_QUEUE_SIZE);
    }
    METRIC_SETUP(_overallRecallMetric, "indexlib.vector.recall_ratio", kmonitor::GAUGE);
    METRIC_SETUP(_realtimeRecallMetric, "indexlib.vector.rt_recall_ratio", kmonitor::GAUGE);
    METRIC_SETUP(_realtimeProportionMetric, "indexlib.vector.rt_proportion", kmonitor::GAUGE);
    return Status::OK();
}

Status AithetaRecallReporter::AsnynReport(const AithetaQueries& indexQuery)
{
    if (!EnableReport() || indexQuery.aithetaqueries_size() <= 0) {
        return Status::OK();
    }
    RecallWorkItem* workItem = new RecallWorkItem([this, indexQuery]() {
        this->DoReport(indexQuery, false);
        this->DoReport(indexQuery, true);
    });

    auto error = _threadPool->pushWorkItem(workItem, /*isBlock=*/false);
    switch (error) {
    case ThreadPool::ERROR_NONE: {
        AUTIL_LOG(DEBUG, "workItem is pushed already");
        return Status::OK();
    }
    case ThreadPool::ERROR_POOL_QUEUE_FULL: {
        delete workItem;
        RETURN_STATUS_ERROR(InternalError, "push recall work item failed, pool is full");
    }
    default: {
        delete workItem;
        RETURN_STATUS_ERROR(InternalError, "unexpected error[%d]", error);
    }
    }
    return Status::OK();
}

void AithetaRecallReporter::DoReport(const AithetaQueries& annQuery, bool onlySearchRt)
{
    if (onlySearchRt && !_indexReader->hasRtSearcher()) {
        return;
    }

    std::unordered_set<docid_t> lrDocSet;
    if (!LRSearch(annQuery, onlySearchRt, lrDocSet).IsOK()) {
        return;
    }

    ResultHolder annResult(_aithetaConfig.distanceType);
    if (!_indexReader->DoSearch(annQuery, nullptr, annResult, onlySearchRt).IsOK()) {
        return;
    }
    size_t topK = AithetaRecallReporter::CalcTopK(annQuery);
    const auto& annMatchItems = annResult.GetTopkMatchItems(topK);

    size_t hitCount = 0;
    for (const auto& annMatchItem : annMatchItems) {
        if (lrDocSet.find(annMatchItem.docid) != lrDocSet.end()) {
            ++hitCount;
        }
    }

    auto knomTags = GetKmonTags(annQuery);
    float recall = hitCount * 1.0f / lrDocSet.size();
    if (onlySearchRt) {
        MetricReport(_realtimeRecallMetric, knomTags, recall);
    } else {
        MetricReport(_overallRecallMetric, knomTags, recall);
    }

    if (onlySearchRt || !_indexReader->hasRtSearcher() || annMatchItems.empty()) {
        return;
    }

    size_t rtMatchCount = 0;
    for (const auto& annMatchItem : annMatchItems) {
        if (annMatchItem.docid >= _indexReader->GetLatestRtBaseDocId()) {
            ++rtMatchCount;
        }
    }
    float proportion = rtMatchCount * 1.0f / annMatchItems.size();
    MetricReport(_realtimeProportionMetric, knomTags, proportion);
}

Status AithetaRecallReporter::LRSearch(const AithetaQueries& rawQuery, bool onlySearchRt,
                                       std::unordered_set<docid_t>& lrDocSet)
{
    AithetaQueries lrQuery(rawQuery);
    for (auto& query : *lrQuery.mutable_aithetaqueries()) {
        query.set_lrsearch(true);
    }
    ResultHolder resultHolder(_aithetaConfig.distanceType);
    auto status = _indexReader->DoSearch(lrQuery, nullptr, resultHolder, onlySearchRt);
    RETURN_IF_STATUS_ERROR(status, "lr search failed.");
    size_t topK = AithetaRecallReporter::CalcTopK(lrQuery);
    const auto& lrMatchItems = resultHolder.GetTopkMatchItems(topK);
    if (lrMatchItems.empty()) {
        return Status::InternalError("lr search result is empty.");
    }
    for (const auto& lrMatchItem : lrMatchItems) {
        lrDocSet.insert(lrMatchItem.docid);
    }
    return Status::OK();
}

void AithetaRecallReporter::Destory()
{
    if (nullptr != _threadPool) {
        _threadPool->stop();
        AUTIL_LOG(INFO, "aitheta recall reporter stop");
    }
}

size_t AithetaRecallReporter::CalcTopK(const AithetaQueries& indexQuery)
{
    size_t topK = 0;
    for (const auto& aithetaQuery : indexQuery.aithetaqueries()) {
        topK += aithetaQuery.topk() * aithetaQuery.embeddingcount();
    }
    return topK;
}

std::shared_ptr<kmonitor::MetricsTags> AithetaRecallReporter::GetKmonTags(const AithetaQueries& aithetaQueries)
{
    if (aithetaQueries.querytags().empty()) {
        _metricReporter->getTags();
    }
    std::stringstream ss;
    for (const auto& entry : aithetaQueries.querytags()) {
        ss << entry.first << "|" << entry.second << ";";
    }
    string key = ss.str();
    auto iter = _kmonTagMap.find(key);
    if (iter != _kmonTagMap.end()) {
        return iter->second;
    }
    std::map<string, string> queryTags(aithetaQueries.querytags().begin(), aithetaQueries.querytags().end());
    auto kmonTags = std::make_shared<kmonitor::MetricsTags>(queryTags);
    _metricReporter->getTags()->MergeTags(kmonTags.get());
    _kmonTagMap[key] = kmonTags;
    return kmonTags;
}

void AithetaRecallReporter::MetricReport(const std::shared_ptr<Metric>& metric,
                                         const std::shared_ptr<kmonitor::MetricsTags>& tags, float value)
{
    if (nullptr != metric) {
        if (nullptr != tags) {
            metric->Report(tags, value);
        } else {
            metric->Report(value);
        }
    }
}

} // namespace indexlibv2::index::ann