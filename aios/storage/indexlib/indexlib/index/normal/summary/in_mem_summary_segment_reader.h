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
#ifndef __INDEXLIB_IN_MEM_SUMMARY_SEGMENT_READER_H
#define __INDEXLIB_IN_MEM_SUMMARY_SEGMENT_READER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/summary_group_config.h"
#include "indexlib/document/index_document/normal_document/summary_group_formatter.h"
#include "indexlib/index/normal/summary/summary_define.h"
#include "indexlib/index/normal/summary/summary_segment_reader.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class InMemSummarySegmentReader : public SummarySegmentReader
{
public:
    InMemSummarySegmentReader(const config::SummaryGroupConfigPtr& summaryGroupConfig, SummaryDataAccessor* accessor);
    ~InMemSummarySegmentReader();

public:
    bool GetDocument(docid_t localDocId, document::SearchSummaryDocument* summaryDoc) const override;

    size_t GetRawDataLength(docid_t localDocId) override
    {
        assert(false);
        return 0;
    }

    void GetRawData(docid_t localDocId, char* rawData, size_t len) override { assert(false); }

    size_t GetDocCount() { return mAccessor->GetDocCount(); }

private:
    config::SummaryGroupConfigPtr mSummaryGroupConfig;
    SummaryDataAccessor* mAccessor;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemSummarySegmentReader);
}} // namespace indexlib::index

#endif //__INDEXLIB_IN_MEM_SUMMARY_SEGMENT_READER_H
