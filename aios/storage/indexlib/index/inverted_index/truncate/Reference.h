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

#include "indexlib/base/FieldType.h"
#include "indexlib/index/inverted_index/truncate/DocInfo.h"

namespace indexlib::index {

class Reference
{
public:
    Reference(size_t offset, FieldType fieldType, bool supportNull)
        : _offset(offset)
        , _supportNull(supportNull)
        , _fieldType(fieldType)
    {
    }

    virtual ~Reference() = default;

public:
    size_t GetOffset() const { return _offset; }
    FieldType GetFieldType() const { return _fieldType; }
    virtual std::string GetStringValue(DocInfo* docInfo) = 0;

protected:
    size_t _offset;
    bool _supportNull;
    FieldType _fieldType;
};

} // namespace indexlib::index
