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
#include "indexlib/index/merger_util/truncate/doc_filter_processor.h"
#include "indexlib/index/merger_util/truncate/doc_info_allocator.h"
#include "indexlib/index/merger_util/truncate/evaluator.h"
#include "indexlib/index/merger_util/truncate/reference.h"
#include "indexlib/indexlib.h"

namespace indexlib::index::legacy {

class DocPayloadFilterProcessor : public DocFilterProcessor
{
public:
    DocPayloadFilterProcessor(const config::DiversityConstrain& constrain,
                              const EvaluatorPtr& evaluator = EvaluatorPtr(),
                              const DocInfoAllocatorPtr& allocator = DocInfoAllocatorPtr());
    ~DocPayloadFilterProcessor();

public:
    bool BeginFilter(const DictKeyInfo& key, const PostingIteratorPtr& postingIt) override;
    void BeginFilter(int64_t minValue, int64_t maxValue,
                     const PostingIteratorPtr& postingIt = PostingIteratorPtr()) override
    {
        mMinValue = minValue;
        mMaxValue = maxValue;
        mPostingIt = postingIt;
    }
    bool IsFiltered(docid_t docId) override;
    void SetTruncateMetaReader(const TruncateMetaReaderPtr& metaReader) override { mMetaReader = metaReader; };

    // for test
    void GetFilterRange(int64_t& min, int64_t& max);

private:
    int64_t mMinValue;
    int64_t mMaxValue;
    uint64_t mMask;
    PostingIteratorPtr mPostingIt;
    TruncateMetaReaderPtr mMetaReader;
    EvaluatorPtr mEvaluator;
    DocInfo* mDocInfo;
    Reference* mSortFieldRef;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocPayloadFilterProcessor);
} // namespace indexlib::index::legacy
