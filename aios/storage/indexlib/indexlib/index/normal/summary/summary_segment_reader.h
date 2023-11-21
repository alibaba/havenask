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
#include "indexlib/document/index_document/normal_document/search_summary_document.h"
#include "indexlib/document/index_document/normal_document/summary_document.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class SummarySegmentReader
{
public:
    SummarySegmentReader() {}
    virtual ~SummarySegmentReader() {}

public:
    /**
     * New get document summary by docid
     */
    virtual bool GetDocument(docid_t localDocId, document::SearchSummaryDocument* summaryDoc) const = 0;

    virtual size_t GetRawDataLength(docid_t localDocId) = 0;

    virtual void GetRawData(docid_t localDocId, char* rawData, size_t len) = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SummarySegmentReader);
}} // namespace indexlib::index
