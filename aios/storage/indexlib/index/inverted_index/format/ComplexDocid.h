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

#include <cassert>

#include "indexlib/base/Types.h"

namespace indexlib::index {

class ComplexDocId
{
public:
    ComplexDocId(docid_t docId, bool remove) : isDelete(remove ? 1 : 0), key(docId) { assert(docId >= 0); }
    constexpr ComplexDocId() : isDelete(0), key(0) {}

    bool IsDelete() const { return isDelete == 1; }
    docid_t DocId() const { return key; }

    bool operator<(const ComplexDocId& other) const { return key < other.key; }
    bool operator==(const ComplexDocId& other) const { return key == other.key; }
    bool operator!=(const ComplexDocId& other) const { return key != other.key; }
    bool operator>(const ComplexDocId& other) const { return key > other.key; }
    bool operator<=(const ComplexDocId& other) const { return key <= other.key; }
    bool operator>=(const ComplexDocId& other) const { return key >= other.key; }

private:
    uint32_t isDelete : 1;
    uint32_t key      : 31;
};
} // namespace indexlib::index
