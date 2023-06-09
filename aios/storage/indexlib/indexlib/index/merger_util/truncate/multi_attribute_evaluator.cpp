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
#include "indexlib/index/merger_util/truncate/multi_attribute_evaluator.h"

using namespace std;

namespace indexlib::index::legacy {
IE_LOG_SETUP(index, MultiAttributeEvaluator);

MultiAttributeEvaluator::MultiAttributeEvaluator() {}

MultiAttributeEvaluator::~MultiAttributeEvaluator() {}

void MultiAttributeEvaluator::AddEvaluator(const EvaluatorPtr& evaluator) { mEvaluators.push_back(evaluator); }

void MultiAttributeEvaluator::Evaluate(docid_t docId, const std::shared_ptr<PostingIterator>& postingIter,
                                       DocInfo* docInfo)
{
    for (size_t i = 0; i < mEvaluators.size(); i++) {
        mEvaluators[i]->Evaluate(docId, postingIter, docInfo);
    }
}
} // namespace indexlib::index::legacy
