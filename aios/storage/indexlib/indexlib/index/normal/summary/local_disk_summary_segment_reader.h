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

#include "fslib/fslib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/summary_schema.h"
#include "indexlib/document/index_document/normal_document/summary_group_formatter.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/data_structure/var_len_data_reader.h"
#include "indexlib/index/normal/summary/summary_define.h"
#include "indexlib/index/normal/summary/summary_segment_reader.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

typedef std::vector<document::SearchSummaryDocument*> SearchSummaryDocVec;

class LocalDiskSummarySegmentReader : public SummarySegmentReader
{
public:
    LocalDiskSummarySegmentReader(const config::SummaryGroupConfigPtr& summaryGroupConfig);
    ~LocalDiskSummarySegmentReader();

public:
    bool Open(const index_base::SegmentData& segmentData);

    bool GetDocument(docid_t localDocId, document::SearchSummaryDocument* summaryDoc) const override;
    future_lite::coro::Lazy<index::ErrorCodeVec> GetDocument(const std::vector<docid_t>& docIds,
                                                             autil::mem_pool::Pool* sessionPool,
                                                             file_system::ReadOption readOption,
                                                             const SearchSummaryDocVec* docs) noexcept;
    size_t GetRawDataLength(docid_t localDocId) override
    {
        assert(false);
        return 0;
    }

    void GetRawData(docid_t localDocId, char* rawData, size_t len) override { assert(false); }

protected:
    VarLenDataReaderPtr mDataReader;
    config::SummaryGroupConfigPtr mSummaryGroupConfig;
    static constexpr size_t DEFAULT_SEGMENT_POOL_SIZE = 512 * 1024;

private:
    friend class LocalDiskSummarySegmentReaderTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LocalDiskSummarySegmentReader);

////////////////////////////////////////////
}} // namespace indexlib::index
