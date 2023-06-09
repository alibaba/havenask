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

#include "indexlib/index/inverted_index/truncate/IEvaluator.h"
#include "indexlib/index/inverted_index/truncate/ReferenceTyped.h"

namespace indexlib::index {

class DocPayloadEvaluator : public IEvaluator
{
public:
    DocPayloadEvaluator(Reference* refer) { _refer = dynamic_cast<ReferenceTyped<double>*>(refer); }
    ~DocPayloadEvaluator() = default;

public:
    void SetFactorEvaluator(std::shared_ptr<IEvaluator> evaluator);
    void SetFP16(bool flag);
    void Evaluate(docid_t docId, const std::shared_ptr<PostingIterator>& postingIter, DocInfo* docInfo) override;
    double GetValue(docid_t docId, const std::shared_ptr<PostingIterator>& postingIter, DocInfo* docInfo) override;

private:
    double EvaluateDocPayload(const std::shared_ptr<PostingIterator>& postingIter);

private:
    ReferenceTyped<double>* _refer;
    std::shared_ptr<IEvaluator> _factorEvaluator;
    bool _useFP16 = false;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
