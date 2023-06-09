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
#include "indexlib/index/merger_util/truncate/doc_payload_filter_processor.h"

#include "autil/StringUtil.h"
#include "indexlib/index/inverted_index/PostingIterator.h"

using namespace std;

namespace indexlib::index::legacy {
IE_LOG_SETUP(index, DocPayloadFilterProcessor);

DocPayloadFilterProcessor::DocPayloadFilterProcessor(const config::DiversityConstrain& constrain,
                                                     const EvaluatorPtr& evaluator,
                                                     const DocInfoAllocatorPtr& allocator)
{
    mMaxValue = constrain.GetFilterMaxValue();
    mMinValue = constrain.GetFilterMinValue();
    mMask = constrain.GetFilterMask();
    mEvaluator = evaluator;
    mSortFieldRef = nullptr;
    mDocInfo = nullptr;
    if (allocator) {
        mSortFieldRef = allocator->GetReference(DOC_PAYLOAD_FIELD_NAME);
        mDocInfo = allocator->Allocate();
    }
}

DocPayloadFilterProcessor::~DocPayloadFilterProcessor() {}

void DocPayloadFilterProcessor::GetFilterRange(int64_t& min, int64_t& max)
{
    min = mMinValue;
    max = mMaxValue;
}

bool DocPayloadFilterProcessor::BeginFilter(const DictKeyInfo& key, const PostingIteratorPtr& postingIt)
{
    mPostingIt = postingIt;
    if (mMetaReader && !mMetaReader->Lookup(key, mMinValue, mMaxValue)) {
        return false;
    }
    return true;
}

bool DocPayloadFilterProcessor::IsFiltered(docid_t docId)
{
    assert(mPostingIt);
    if (mEvaluator) {
        mDocInfo->SetDocId(docId);
        mEvaluator->Evaluate(docId, mPostingIt, mDocInfo);
        string valueStr = mSortFieldRef->GetStringValue(mDocInfo);
        double value;
        if (!autil::StringUtil::fromString(valueStr, value)) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "invalid value: %s", valueStr.c_str());
        }
        int64_t valueInt64 = (int64_t)value;
        valueInt64 &= mMask;
        return !(mMinValue <= valueInt64 && valueInt64 <= mMaxValue);
    } else {
        docpayload_t value = mPostingIt->GetDocPayload();
        value &= (docpayload_t)mMask;
        return !(mMinValue <= (int64_t)value && (int64_t)value <= mMaxValue);
    }
}
} // namespace indexlib::index::legacy
