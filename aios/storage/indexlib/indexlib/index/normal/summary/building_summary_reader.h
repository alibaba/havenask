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

class BuildingSummaryReader
{
public:
    BuildingSummaryReader();
    ~BuildingSummaryReader();

public:
    void AddSegmentReader(docid_t baseDocId, const InMemSummarySegmentReaderPtr& inMemSegReader)
    {
        if (inMemSegReader) {
            mBaseDocIds.push_back(baseDocId);
            mSegmentReaders.push_back(inMemSegReader);
        }
    }

    bool GetDocument(docid_t docId, document::SearchSummaryDocument* summaryDoc) const
    {
        for (size_t i = 0; i < mSegmentReaders.size(); i++) {
            docid_t curBaseDocId = mBaseDocIds[i];
            if (docId < curBaseDocId) {
                return false;
            }
            if (mSegmentReaders[i]->GetDocument(docId - curBaseDocId, summaryDoc)) {
                return true;
            }
        }
        return false;
    }

private:
    std::vector<docid_t> mBaseDocIds;
    std::vector<InMemSummarySegmentReaderPtr> mSegmentReaders;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BuildingSummaryReader);
}} // namespace indexlib::index
