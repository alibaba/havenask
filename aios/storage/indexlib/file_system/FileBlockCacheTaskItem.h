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

#include "autil/Log.h"
#include "indexlib/util/TaskItem.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "indexlib/util/metrics/Monitor.h"

namespace indexlib::util {
class BlockCache;
}

namespace indexlib { namespace file_system {

class FileBlockCacheTaskItem : public util::TaskItem
{
public:
    FileBlockCacheTaskItem(util::BlockCache* blockCache, util::MetricProviderPtr metricProvider, bool isGlobal,
                           const kmonitor::MetricsTags& metricsTags);
    ~FileBlockCacheTaskItem() = default;

public:
    void Run() override;

private:
    void DeclareMetrics(bool isGlobal);
    void ReportMetrics();

private:
    util::BlockCache* _blockCache;
    util::MetricProviderPtr _metricProvider;
    bool _isGlobal;
    kmonitor::MetricsTags _metricsTags;

    IE_DECLARE_METRIC(BlockCacheMemUse);
    IE_DECLARE_METRIC(BlockCacheDiskUse);

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FileBlockCacheTaskItem> FileBlockCacheTaskItemPtr;
}} // namespace indexlib::file_system
