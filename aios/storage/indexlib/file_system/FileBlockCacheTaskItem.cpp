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
#include "indexlib/file_system/FileBlockCacheTaskItem.h"

#include <iosfwd>
#include <memory>

#include "indexlib/util/cache/BlockCache.h"
#include "indexlib/util/metrics/Metric.h"
#include "kmonitor/client/MetricType.h"

using namespace std;

using namespace indexlib::util;
namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, FileBlockCacheTaskItem);

FileBlockCacheTaskItem::FileBlockCacheTaskItem(util::BlockCache* blockCache, util::MetricProviderPtr metricProvider,
                                               bool isGlobal, const kmonitor::MetricsTags& metricsTags)
    : _blockCache(blockCache)
    , _metricProvider(metricProvider)
    , _isGlobal(isGlobal)
    , _metricsTags(metricsTags)
{
    DeclareMetrics(_isGlobal);
}

void FileBlockCacheTaskItem::Run() { ReportMetrics(); }

void FileBlockCacheTaskItem::DeclareMetrics(bool isGlobal)
{
    std::string prefix = "global";
    if (!isGlobal) {
        prefix = "local";
    }
    IE_INIT_METRIC_GROUP(_metricProvider, BlockCacheMemUse, prefix + "/BlockCacheMemUse", kmonitor::GAUGE, "byte");
    IE_INIT_METRIC_GROUP(_metricProvider, BlockCacheDiskUse, prefix + "/BlockCacheDiskUse", kmonitor::GAUGE, "byte");
}

void FileBlockCacheTaskItem::ReportMetrics()
{
    assert(_metricsTags.Size() != 0);
    auto resourceInfo = _blockCache->GetResourceInfo();
    IE_REPORT_METRIC_WITH_TAGS(BlockCacheMemUse, &_metricsTags, resourceInfo.memoryUse);
    IE_REPORT_METRIC_WITH_TAGS(BlockCacheDiskUse, &_metricsTags, resourceInfo.diskUse);
    _blockCache->ReportMetrics();
}
}} // namespace indexlib::file_system
