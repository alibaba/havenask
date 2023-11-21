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

#include "indexlib/common_define.h"
#include "indexlib/index/normal/summary/in_mem_summary_segment_reader.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class InMemSummarySegmentReaderContainer : public SummarySegmentReader
{
public:
    InMemSummarySegmentReaderContainer();
    ~InMemSummarySegmentReaderContainer();

public:
    const InMemSummarySegmentReaderPtr& GetInMemSummarySegmentReader(summarygroupid_t summaryGroupId) const;

    void AddReader(const SummarySegmentReaderPtr& summarySegmentReader);

public:
    bool GetDocument(docid_t localDocId, document::SearchSummaryDocument* summaryDoc) const override
    {
        assert(false);
        return false;
    }

    size_t GetRawDataLength(docid_t localDocId) override
    {
        assert(false);
        return 0;
    }

    void GetRawData(docid_t localDocId, char* rawData, size_t len) override { assert(false); }

private:
    typedef std::vector<InMemSummarySegmentReaderPtr> InMemSummarySegmentReaderVec;
    InMemSummarySegmentReaderVec mInMemSummarySegmentReaderVec;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemSummarySegmentReaderContainer);
}} // namespace indexlib::index
