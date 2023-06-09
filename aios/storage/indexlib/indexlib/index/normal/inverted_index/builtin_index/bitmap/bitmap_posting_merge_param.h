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
#ifndef __INDEXLIB_BITMAP_POSTING_MERGE_PARAM_H
#define __INDEXLIB_BITMAP_POSTING_MERGE_PARAM_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/file_system/ByteSliceWriter.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index { namespace legacy {

struct BitmapPostingMergeParam {
public:
    BitmapPostingMergeParam(docid_t oldBaseId, docid_t newBaseId, const TermMeta* tm, const util::ByteSliceList* bsl)
        : oldBaseDocId(oldBaseId)
        , newBaseDocId(newBaseId)
        , termMeta(tm)
        , postList(bsl)
    {
    }

    ~BitmapPostingMergeParam() {}

public:
    docid_t oldBaseDocId;
    docid_t newBaseDocId;
    const TermMeta* termMeta;
    const util::ByteSliceList* postList;
};

typedef std::shared_ptr<BitmapPostingMergeParam> BitmapPostingMergeParamPtr;
}}} // namespace indexlib::index::legacy

#endif //__INDEXLIB_BITMAP_POSTING_MERGE_PARAM_H
