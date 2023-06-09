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
#include "indexlib/index/inverted_index/AndPostingExecutor.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, AndPostingExecutor);

AndPostingExecutor::AndPostingExecutor(const std::vector<std::shared_ptr<PostingExecutor>>& postingExecutors)
    : _postingExecutors(postingExecutors)
{
    sort(_postingExecutors.begin(), _postingExecutors.end(), DFCompare());
}

AndPostingExecutor::~AndPostingExecutor() {}

df_t AndPostingExecutor::GetDF() const
{
    df_t minDF = std::numeric_limits<df_t>::max();
    for (size_t i = 0; i < _postingExecutors.size(); ++i) {
        minDF = std::min(minDF, _postingExecutors[i]->GetDF());
    }
    return minDF;
}

docid_t AndPostingExecutor::DoSeek(docid_t id)
{
    auto firstIter = _postingExecutors.begin();
    auto currentIter = firstIter;
    auto endIter = _postingExecutors.end();

    docid_t current = id;
    do {
        docid_t tmpId = (*currentIter)->Seek(current);
        if (tmpId != current) {
            current = tmpId;
            currentIter = firstIter;
        } else {
            ++currentIter;
        }
    } while (END_DOCID != current && currentIter != endIter);
    return current;
}
} // namespace indexlib::index
