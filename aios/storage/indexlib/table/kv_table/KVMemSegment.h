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
#include "indexlib/table/kv_table/KVMemSegment.h"
#include "indexlib/table/plain/PlainMemSegment.h"

namespace indexlibv2::table {

class KVMemSegment : public plain::PlainMemSegment
{
public:
    KVMemSegment(const config::TabletOptions* options, const std::shared_ptr<config::ITabletSchema>& schema,
                 const framework::SegmentMeta& segmentMeta, bool enableMemoryReclaim);
    ~KVMemSegment() {}

protected:
    const std::vector<std::shared_ptr<indexlibv2::index::IMemIndexer>>& GetBuildIndexers() override;
    std::pair<Status, size_t> GetLastSegmentMemUse(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                   const indexlib::framework::SegmentMetrics* metrics) const override;
    void PrepareIndexerParameter(const framework::BuildResource& resource, int64_t maxMemoryUseInBytes,
                                 index::IndexerParameter& indexerParam) const override;

private:
    bool _hasCheckPKValueIndex = false;
    bool _enableMemoryReclaim = false;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
