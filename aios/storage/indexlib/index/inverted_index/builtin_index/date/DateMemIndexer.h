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
#include "indexlib/index/inverted_index/InvertedMemIndexer.h"
#include "indexlib/index/inverted_index/config/DateLevelFormat.h"

namespace indexlib::index {
class TimeRangeInfo;
class InvertedIndexMetrics;

class DateMemIndexer : public InvertedMemIndexer
{
public:
    explicit DateMemIndexer(const indexlibv2::index::IndexerParameter& indexerParam,
                            const std::shared_ptr<InvertedIndexMetrics>& metrics);
    ~DateMemIndexer();
    Status Init(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                indexlibv2::document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory) override;
    Status AddField(const document::Field* field) override;
    std::shared_ptr<IndexSegmentReader> CreateInMemReader() override;

protected:
    Status DoDump(autil::mem_pool::PoolBase* dumpPool, const std::shared_ptr<file_system::Directory>& indexDirectory,
                  const std::shared_ptr<indexlibv2::framework::DumpParams>& dumpParams) override;

private:
    config::DateLevelFormat _format;
    std::shared_ptr<TimeRangeInfo> _timeRangeInfo;
    uint64_t _granularityLevel;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
