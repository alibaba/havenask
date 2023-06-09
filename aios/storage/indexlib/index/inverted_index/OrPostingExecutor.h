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
#include <queue>

#include "autil/Log.h"
#include "indexlib/index/inverted_index/PostingExecutor.h"

namespace indexlib::index {

class OrPostingExecutor : public PostingExecutor
{
public:
    OrPostingExecutor(const std::vector<std::shared_ptr<PostingExecutor>>& postingExecutors);
    ~OrPostingExecutor();

public:
    df_t GetDF() const override;
    docid_t DoSeek(docid_t docId) override;

private:
    struct PostingExecutorEntry {
        PostingExecutorEntry() : docId(END_DOCID), entryId(0) {}
        docid_t docId;
        uint32_t entryId;
    };

    class EntryItemComparator
    {
    public:
        bool operator()(const PostingExecutorEntry& lhs, const PostingExecutorEntry& rhs)
        {
            return (lhs.docId != rhs.docId) ? lhs.docId > rhs.docId : lhs.entryId > rhs.entryId;
        }
    };

    using PostingExecutorEntryHeap =
        std::priority_queue<PostingExecutorEntry, std::vector<PostingExecutorEntry>, EntryItemComparator>;

private:
    std::vector<std::shared_ptr<PostingExecutor>> _postingExecutors;
    PostingExecutorEntryHeap _entryHeap;
    docid_t _currentDocId;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
