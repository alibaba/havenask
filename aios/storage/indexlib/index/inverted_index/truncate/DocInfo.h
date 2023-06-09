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

#include "autil/StringUtil.h"
#include "indexlib/base/Types.h"

namespace indexlib::index {

class DocInfo
{
public:
    void SetDocId(docid_t docId) { _docId = docId; }

    docid_t IncDocId() { return ++_docId; }

    docid_t GetDocId() const { return _docId; }

    uint8_t* Get(size_t offset) { return (uint8_t*)this + offset; }

    const uint8_t* Get(size_t offset) const { return (uint8_t*)this + offset; }

private:
    docid_t _docId;
};

} // namespace indexlib::index
