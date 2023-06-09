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
#include "indexlib/index/common/field_format/range/RangeFieldEncoder.h"
#include "indexlib/index/inverted_index/InvertedLeafReader.h"
#include "indexlib/index/inverted_index/SegmentPostings.h"

namespace indexlib::index {

class RangeLevelLeafReader : public InvertedLeafReader
{
public:
    RangeLevelLeafReader(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                         const IndexFormatOption& formatOption, uint64_t docCount,
                         const std::shared_ptr<DictionaryReader>& dictReader,
                         const std::shared_ptr<file_system::FileReader>& postingReader);
    ~RangeLevelLeafReader();

    future_lite::coro::Lazy<index::ErrorCode>
    FillSegmentPostings(const RangeFieldEncoder::Ranges& ranges, docid_t baseDocId,
                        const std::shared_ptr<SegmentPostings>& segmentPostings, autil::mem_pool::Pool* sessionPool,
                        file_system::ReadOption option, InvertedIndexSearchTracer* tracer) noexcept;

private:
    future_lite::coro::Lazy<index::Result<SegmentPosting>> FillOneSegment(dictvalue_t value, docid_t baseDocId,
                                                                          autil::mem_pool::Pool* sessionPool,
                                                                          file_system::ReadOption option,
                                                                          InvertedIndexSearchTracer* tracer) noexcept;

private:
    bool _isHashTypeDict = false;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
