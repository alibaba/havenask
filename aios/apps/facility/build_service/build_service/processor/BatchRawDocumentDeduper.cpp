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
#include "build_service/processor/BatchRawDocumentDeduper.h"

#include "build_service/util/Monitor.h"
#include "indexlib/util/metrics/Metric.h"
#include "indexlib/util/metrics/MetricProvider.h"

using namespace std;
using namespace build_service::document;
using namespace indexlib::util;

namespace build_service { namespace processor {
BS_LOG_SETUP(processor, BatchRawDocumentDeduper);

BatchRawDocumentDeduper::BatchRawDocumentDeduper(const std::string& dedupField,
                                                 const indexlib::util::MetricProviderPtr& metricProvider)
    : _dedupField(dedupField)
{
    if (metricProvider) {
        _dedupDocQpsMetric = DECLARE_METRIC(metricProvider, "perf/batchDedupDocQps", kmonitor::QPS, "qps");
        _dedupProcessLatencyMetric = DECLARE_METRIC(metricProvider, "perf/batchDedupLatency", kmonitor::GAUGE, "ms");
    }
}

BatchRawDocumentDeduper::~BatchRawDocumentDeduper() {}

// TODO: support merge update & add doc, with modify_fields & modify_values updated
void BatchRawDocumentDeduper::process(const document::RawDocumentVecPtr& docQueue)
{
    assert(!_dedupField.empty());
    if (docQueue == nullptr || docQueue->size() <= 1) {
        return;
    }

    ScopeLatencyReporter scopeTime(_dedupProcessLatencyMetric.get());
    autil::StringView fieldName = autil::StringView(_dedupField);
    unordered_map<autil::StringView, std::pair<int32_t, int32_t>> uniqMap;
    for (int i = docQueue->size() - 1; i >= 0; i--) {
        auto& rawDoc = (*docQueue)[i];
        if (!rawDoc) {
            continue;
        }
        auto docType = rawDoc->getDocOperateType();
        if (docType != indexlib::ADD_DOC && docType != indexlib::DELETE_DOC && docType != indexlib::UPDATE_FIELD) {
            continue;
        }
        autil::StringView constStr = rawDoc->getField(fieldName);
        if (constStr.empty() || constStr.data() == nullptr) {
            continue;
        }
        auto iter = uniqMap.find(constStr);
        if (iter != uniqMap.end()) {
            iter->second.first = i;
            rawDoc->setDocOperateType(indexlib::SKIP_DOC); // skip duplicated doc
            INCREASE_QPS(_dedupDocQpsMetric);
            continue;
        }
        if (docType == indexlib::ADD_DOC) {
            // rewrite modifyFields info, dedup doc will disable add2update function
            rawDoc->setField(document::HA_RESERVED_MODIFY_FIELDS, autil::StringView::empty_instance());
            rawDoc->setField(document::HA_RESERVED_MODIFY_VALUES, autil::StringView::empty_instance());
            uniqMap.insert(make_pair(constStr, make_pair(i, i)));
        }
        if (docType == indexlib::DELETE_DOC) {
            uniqMap.insert(make_pair(constStr, make_pair(i, i)));
        }
    }
    for (auto iter = uniqMap.begin(); iter != uniqMap.end(); iter++) {
        if (iter->second.first == iter->second.second) {
            continue; // no deplicated doc to skip
        }
        RawDocumentPtr& lastDoc = (*docQueue)[iter->second.second];
        if (lastDoc->getDocOperateType() != indexlib::ADD_DOC) {
            continue;
        }
        RawDocumentPtr& firstDoc = (*docQueue)[iter->second.first];
        // no-dedup addDoc may trigger modifyFields
        // change last add to the first skip doc position for failover
        indexlibv2::framework::Locator lastLocator = lastDoc->getLocator();
        lastDoc->SetLocator(firstDoc->getLocator());
        firstDoc->SetLocator(lastLocator);
        swap((*docQueue)[iter->second.first], (*docQueue)[iter->second.second]);
    }
}

}} // namespace build_service::processor
