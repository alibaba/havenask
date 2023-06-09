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

#include <memory>

#include "autil/Log.h"
#include "indexlib/util/TaskItem.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "indexlib/util/metrics/Monitor.h"

namespace indexlib { namespace util {

class SearchCache;

class SearchCacheTaskItem : public TaskItem
{
public:
    SearchCacheTaskItem(util::SearchCache* searchCache, util::MetricProviderPtr metricProvider);
    ~SearchCacheTaskItem();

public:
    void Run() override;

private:
    void DeclareMetrics();
    void ReportMetrics();

private:
    util::SearchCache* _searchCache;
    util::MetricProviderPtr _metricProvider;

    IE_DECLARE_METRIC(SearchCacheMemUse);

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SearchCacheTaskItem> SearchCacheTaskItemPtr;
}} // namespace indexlib::util
