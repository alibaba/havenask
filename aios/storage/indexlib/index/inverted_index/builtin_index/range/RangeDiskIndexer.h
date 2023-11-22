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
#include "indexlib/index/inverted_index/InvertedDiskIndexer.h"
#include "indexlib/index/inverted_index/builtin_index/range/RangeInfo.h"

namespace indexlib::index {
class RangeLeafReader;

class RangeDiskIndexer : public InvertedDiskIndexer
{
public:
    explicit RangeDiskIndexer(const indexlibv2::index::DiskIndexerParameter& indexerParam);
    ~RangeDiskIndexer();

    Status Open(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                const std::shared_ptr<file_system::IDirectory>& indexDirectory) override;
    size_t EstimateMemUsed(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                           const std::shared_ptr<file_system::IDirectory>& indexDirectory) override;
    size_t EvaluateCurrentMemUsed() override;

    std::shared_ptr<InvertedLeafReader> GetReader() const override;

private:
    RangeInfo _rangeInfo;
    std::shared_ptr<InvertedDiskIndexer> _bottomLevelDiskIndexer;
    std::shared_ptr<InvertedDiskIndexer> _highLevelDiskIndexer;
    std::shared_ptr<RangeLeafReader> _rangeLeafReader;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
