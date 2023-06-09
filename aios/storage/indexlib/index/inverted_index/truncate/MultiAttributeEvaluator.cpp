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
#include "indexlib/index/inverted_index/truncate/MultiAttributeEvaluator.h"

namespace indexlib::index {

AUTIL_LOG_SETUP(index, MultiAttributeEvaluator);

void MultiAttributeEvaluator::AddEvaluator(const std::shared_ptr<IEvaluator>& evaluator)
{
    _evaluators.push_back(evaluator);
}

void MultiAttributeEvaluator::Evaluate(docid_t docId, const std::shared_ptr<PostingIterator>& postingIter,
                                       DocInfo* docInfo)
{
    for (auto i = 0; i < _evaluators.size(); ++i) {
        _evaluators[i]->Evaluate(docId, postingIter, docInfo);
    }
}

} // namespace indexlib::index
