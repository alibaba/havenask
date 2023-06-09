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
#include "indexlib/index/merger_util/truncate/doc_payload_evaluator.h"

using namespace std;

namespace indexlib::index::legacy {
IE_LOG_SETUP(index, DocPayloadEvaluator);

DocPayloadEvaluator::DocPayloadEvaluator() {}

DocPayloadEvaluator::~DocPayloadEvaluator() {}

void DocPayloadEvaluator::Init(Reference* refer) { mRefer = dynamic_cast<ReferenceTyped<double>*>(refer); }

void DocPayloadEvaluator::SetFactorEvaluator(EvaluatorPtr evaluator) { mFactorEvaluator = evaluator; }

void DocPayloadEvaluator::SetFP16(bool flag) { mUseFP16 = flag; }

double DocPayloadEvaluator::EvaluateDocPayload(const std::shared_ptr<PostingIterator>& postingIter)
{
    if (mUseFP16) {
        docpayload_t payload = postingIter->GetDocPayload();
        float ret;
        uint16_t* dst = reinterpret_cast<uint16_t*>(&ret);
        uint16_t* src = reinterpret_cast<uint16_t*>(&payload);
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
        {
            dst[0] = *src;
            dst[1] = 0;
        }
#else
        {
            dst[0] = 0;
            dst[1] = *src;
        }
#endif
        return ret;
    } else {
        return (double)postingIter->GetDocPayload();
    }
}
void DocPayloadEvaluator::Evaluate(docid_t docId, const std::shared_ptr<PostingIterator>& postingIter, DocInfo* docInfo)
{
    double docPayload = GetValue(docId, postingIter, docInfo);
    mRefer->Set(docPayload, false, docInfo);
}

double DocPayloadEvaluator::GetValue(docid_t docId, const std::shared_ptr<PostingIterator>& postingIter,
                                     DocInfo* docInfo)
{
    double docPayload = EvaluateDocPayload(postingIter);
    if (mFactorEvaluator) {
        docPayload *= mFactorEvaluator->GetValue(docId, postingIter, docInfo);
    }
    return docPayload;
}
} // namespace indexlib::index::legacy
