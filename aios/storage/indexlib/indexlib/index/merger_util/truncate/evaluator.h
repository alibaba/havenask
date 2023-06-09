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
#ifndef __INDEXLIB_EVALUATOR_H
#define __INDEXLIB_EVALUATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/merger_util/truncate/doc_info.h"
#include "indexlib/indexlib.h"

namespace indexlib::index::legacy {

class Evaluator
{
public:
    Evaluator() {}
    virtual ~Evaluator() {}

public:
    virtual void Evaluate(docid_t docId, const std::shared_ptr<PostingIterator>& postingIter, DocInfo* docInfo) = 0;
    virtual double GetValue(docid_t docId, const std::shared_ptr<PostingIterator>& postingIter, DocInfo* docInfo)
    {
        return 0;
    }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(Evaluator);
} // namespace indexlib::index::legacy

#endif //__INDEXLIB_EVALUATOR_H
