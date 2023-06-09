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
#include "indexlib/index/inverted_index/OrPostingExecutor.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, OrPostingExecutor);

OrPostingExecutor::OrPostingExecutor(const std::vector<std::shared_ptr<PostingExecutor>>& postingExecutors)
    : _postingExecutors(postingExecutors)
    , _currentDocId(END_DOCID)
{
    for (size_t i = 0; i < _postingExecutors.size(); i++) {
        docid_t minDocId = _postingExecutors[i]->Seek(0);
        _currentDocId = std::min(minDocId, _currentDocId);

        PostingExecutorEntry entry;
        entry.docId = minDocId;
        entry.entryId = (uint32_t)i;
        _entryHeap.push(entry);
    }
}

OrPostingExecutor::~OrPostingExecutor() {}

df_t OrPostingExecutor::GetDF() const
{
    df_t df = 0;
    for (size_t i = 0; i < _postingExecutors.size(); i++) {
        df = std::max(df, _postingExecutors[i]->GetDF());
    }
    return df;
}

docid_t OrPostingExecutor::DoSeek(docid_t id)
{
    if (_currentDocId == END_DOCID) {
        return END_DOCID;
    }

    if (id > _currentDocId) {
        while (!_entryHeap.empty()) {
            PostingExecutorEntry entry = _entryHeap.top();
            if (entry.docId <= _currentDocId) {
                _entryHeap.pop();
                entry.docId = _postingExecutors[entry.entryId]->Seek(id);
                _entryHeap.push(entry);
                continue;
            }
            _currentDocId = entry.docId;
            break;
        }
    }
    return _currentDocId;
}
} // namespace indexlib::index
