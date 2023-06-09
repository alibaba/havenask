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
#include "indexlib/util/cache/SearchCacheTaskItem.h"

#include "indexlib/util/cache/SearchCache.h"

using namespace std;

namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, SearchCacheTaskItem);

SearchCacheTaskItem::SearchCacheTaskItem(util::SearchCache* searchCache, util::MetricProviderPtr metricProvider)
    : _searchCache(searchCache)
    , _metricProvider(metricProvider)
{
    DeclareMetrics();
}

SearchCacheTaskItem::~SearchCacheTaskItem() {}

void SearchCacheTaskItem::Run() { ReportMetrics(); }

void SearchCacheTaskItem::DeclareMetrics()
{
    IE_INIT_METRIC_GROUP(_metricProvider, SearchCacheMemUse, "global/SearchCacheMemUse", kmonitor::STATUS, "byte");
}

void SearchCacheTaskItem::ReportMetrics()
{
    IE_REPORT_METRIC(SearchCacheMemUse, _searchCache->GetUsage());
    _searchCache->ReportMetrics();
}
}} // namespace indexlib::util
