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
#include "suez/search/CounterReporter.h"

#include "autil/StringUtil.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/TabletInfos.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/util/counter/Counter.h"
#include "indexlib/util/counter/CounterMap.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;
using namespace kmonitor;

namespace suez {

CounterReporter::CounterReporter() {}

CounterReporter::~CounterReporter() {}

void CounterReporter::init(const KMonitorMetaInfo &kmonMetaInfo) {
    if (kmonMetaInfo.metricsPrefix.empty()) {
        _metricsReporter.reset(new MetricsReporter("indexlibsuez", {}, "indexlib_countor"));
    } else {
        _metricsReporter.reset(new MetricsReporter(kmonMetaInfo.metricsPrefix, "indexlibsuez", {}, "indexlib_countor"));
    }
}

void CounterReporter::report(const IndexProvider &indexProvider) {
    for (const auto &kv : indexProvider.tableReader) {
        if (kv.second) {
            reportOneTableMetrics(kv.first, kv.second->GetCounterMap());
        }
    }
    for (const auto &kv : indexProvider.tabletReader) {
        if (kv.second) {
            const auto *tabletInfos = kv.second->GetTabletInfos();
            if (tabletInfos) {
                reportOneTableMetrics(kv.first, tabletInfos->GetCounterMap());
            }
        }
    }
}

void CounterReporter::convertCounterPath(const string &counterPath, string &metricName, map<string, string> &tagsMap) {
    vector<string> pathVec = StringUtil::split(counterPath, ".");
    if (pathVec.size() == 3) {
        if (pathVec[1] == "updateField") {
            tagsMap["field"] = pathVec[2];
            pathVec.pop_back();
            metricName = StringUtil::toString(pathVec, ".");
            return;
        }
    } else if (pathVec.size() == 4) {
        tagsMap["field"] = pathVec[3];
        pathVec.pop_back();
        metricName = StringUtil::toString(pathVec, ".");
        return;
    } else if (pathVec.size() == 5) {
        tagsMap["field"] = pathVec[3];
        tagsMap["temperature"] = pathVec[4];
        pathVec.pop_back();
        pathVec.pop_back();
        metricName = StringUtil::toString(pathVec, ".");
        return;
    }
    metricName = counterPath;
}

void CounterReporter::reportOneTableMetrics(const PartitionId &partId, const CounterMapPtr &counterMap) {
    string nodePath;
    if (!counterMap) {
        return;
    }
    int32_t fullVersionId = partId.tableId.fullVersion;
    string fullVersionIdStr = StringUtil::toString(fullVersionId);
    const string &tableName = partId.getTableName();
    MetricsTags baseTags = {{{"table", tableName}, {"generation_id", to_string(fullVersionId)}}};
    map<string, string> tagsMap;
    std::vector<CounterBasePtr> counterBases = counterMap->FindCounters("");
    for (const auto &counterBase : counterBases) {
        string metricName;
        tagsMap.clear();
        convertCounterPath(counterBase->GetPath(), metricName, tagsMap);
        int64_t counterValue = -1;
        switch (counterBase->GetType()) {
        case CounterBase::CounterType::CT_STATE:
        case CounterBase::CounterType::CT_ACCUMULATIVE:
        case CounterBase::CounterType::CT_DIRECTORY: {
            CounterPtr counter = std::dynamic_pointer_cast<Counter>(counterBase);
            assert(counter != nullptr);
            counterValue = counter->Get();
            break;
        }
        default:
            continue;
        }

        if (tagsMap.empty()) {
            REPORT_USER_MUTABLE_METRIC_TAGS(_metricsReporter, metricName, counterValue, &baseTags);
        } else {
            MetricsTags tags(tagsMap);
            baseTags.MergeTags(&tags);
            REPORT_USER_MUTABLE_METRIC_TAGS(_metricsReporter, metricName, counterValue, &tags);
        }
    }
}

} // namespace suez
