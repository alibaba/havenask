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
#include <memory>

#include "indexlib/base/Constant.h"

namespace indexlib::index {

class InDocSectionMeta;
class SectionAttributeReader
{
public:
    SectionAttributeReader() {}
    virtual ~SectionAttributeReader() {}

public:
    virtual std::shared_ptr<InDocSectionMeta> GetSection(docid_t docId) const = 0;
    virtual fieldid_t GetFieldId(int32_t fieldIdxInPack) const
    {
        assert(false);
        return INVALID_FIELDID;
    }
};

typedef std::shared_ptr<SectionAttributeReader> SectionAttributeReaderPtr;
} // namespace indexlib::index
