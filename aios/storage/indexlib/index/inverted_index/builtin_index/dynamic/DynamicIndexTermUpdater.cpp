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
#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicIndexTermUpdater.h"

#include "indexlib/document/normal/ModifiedTokens.h"
namespace indexlib::index {

DynamicIndexTermUpdater::DynamicIndexTermUpdater(index::DictKeyInfo termKey, bool isNumberIndex,
                                                 DynamicMemIndexer::DynamicPostingTable* postingTable)
    : _termKey(termKey)
    , _isNumberIndex(isNumberIndex)
    , _postingTable(postingTable)
{
}

DynamicIndexTermUpdater::DynamicIndexTermUpdater(DynamicIndexTermUpdater&& other)
    : _termKey(other._termKey)
    , _isNumberIndex(other._isNumberIndex)
    , _postingTable(std::exchange(other._postingTable, nullptr))
{
}

DynamicIndexTermUpdater& DynamicIndexTermUpdater::operator=(DynamicIndexTermUpdater&& other)
{
    if (&other != this) {
        _termKey = other._termKey;
        _isNumberIndex = other._isNumberIndex;
        _postingTable = std::exchange(other._postingTable, nullptr);
    }
    return *this;
}

void DynamicIndexTermUpdater::Update(docid_t localDocId, bool isDelete)
{
    if (_postingTable) {
        auto op = isDelete ? document::ModifiedTokens::Operation::REMOVE : document::ModifiedTokens::Operation::ADD;
        DynamicMemIndexer::UpdateToken(_termKey, _isNumberIndex, _postingTable, localDocId, op);
    }
}

bool DynamicIndexTermUpdater::Empty() const { return _postingTable == nullptr; }

} // namespace indexlib::index
