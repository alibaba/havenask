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
#ifndef __INDEXLIB_TRUNC_WORK_ITEM_H
#define __INDEXLIB_TRUNC_WORK_ITEM_H

#include <memory>

#include "autil/WorkItem.h"
#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/merger_util/truncate/single_truncate_index_writer.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/resource_control_work_item.h"

namespace indexlib::index::legacy {

class TruncWorkItem : public autil::WorkItem
{
public:
    TruncWorkItem(const index::DictKeyInfo& dictKey, const PostingIteratorPtr& postingIt,
                  const TruncateIndexWriterPtr& indexWriter);
    ~TruncWorkItem();

public:
    void process() override;

private:
    index::DictKeyInfo mDictKey;
    PostingIteratorPtr mPostingIt;
    TruncateIndexWriterPtr mIndexWriter;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncWorkItem);
} // namespace indexlib::index::legacy

#endif //__INDEXLIB_TRUNC_WORK_ITEM_H
