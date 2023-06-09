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

#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicMemIndexer.h"

namespace indexlib::index {

class DynamicIndexTermUpdater
{
public:
    DynamicIndexTermUpdater(index::DictKeyInfo termKey, bool isNumberIndex,
                            DynamicMemIndexer::DynamicPostingTable* postingTable);
    ~DynamicIndexTermUpdater() = default;

public:
    DynamicIndexTermUpdater(const DynamicIndexTermUpdater&) = delete;
    DynamicIndexTermUpdater(DynamicIndexTermUpdater&& other);
    DynamicIndexTermUpdater& operator=(DynamicIndexTermUpdater&& other);

public:
    void Update(docid_t localDocId, bool isDelete);
    bool Empty() const;

private:
    index::DictKeyInfo _termKey;
    bool _isNumberIndex = false;
    DynamicMemIndexer::DynamicPostingTable* _postingTable = nullptr;
};

} // namespace indexlib::index
