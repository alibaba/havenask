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

#include "autil/Log.h"
#include "indexlib/base/Define.h"
#include "indexlib/document/normal/SummaryDocument.h"

namespace indexlib::document {

class FieldMetaDocument
{
public:
    FieldMetaDocument(autil::mem_pool::Pool* pool = NULL) {}
    ~FieldMetaDocument() = default;

    using NormalFieldMetaDocument = SummaryDocument;

public:
    const autil::StringView& GetField(fieldid_t id, bool& isNull) const;
    void SetField(fieldid_t id, const autil::StringView& value, bool isNull);
    void serialize(autil::DataBuffer& dataBuffer) const;
    void deserialize(autil::DataBuffer& dataBuffer, autil::mem_pool::Pool* pool, uint32_t docVersion);

public:
    bool operator==(const FieldMetaDocument& right) const;
    bool operator!=(const FieldMetaDocument& right) const;

private:
    NormalFieldMetaDocument _fieldMetaDoc;
    std::set<fieldid_t> _nullFields;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::document
