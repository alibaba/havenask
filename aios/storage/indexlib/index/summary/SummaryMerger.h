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
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/summary/Types.h"

namespace indexlibv2::config {
class SummaryIndexConfig;
}

namespace indexlibv2::index {
class LocalDiskSummaryMerger;
class DocMapper;

class SummaryMerger : public IIndexMerger
{
public:
    SummaryMerger() = default;
    ~SummaryMerger() = default;

public:
    // inherit from IIndexMerger
    Status Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const std::map<std::string, std::any>& params) override;
    Status Merge(const SegmentMergeInfos& segMergeInfos,
                 const std::shared_ptr<framework::IndexTaskResourceManager>& taskResourceManager) override;

private:
    Status DoMerge(const SegmentMergeInfos& segMergeInfos, const std::shared_ptr<DocMapper>& docMapper);

private:
    std::string _docMapperName;
    std::shared_ptr<config::SummaryIndexConfig> _summaryIndexConfig;
    std::map<summarygroupid_t, std::shared_ptr<LocalDiskSummaryMerger>> _groupSummaryMergers;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
