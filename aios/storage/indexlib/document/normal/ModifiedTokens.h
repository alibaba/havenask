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

#include "autil/DataBuffer.h"
#include "indexlib/base/Constant.h"
#include "indexlib/document/normal/Token.h"

namespace indexlib::document {

// ModifiedTokens contains terms and their corresponding operation(add/remove) for a field. It's used to represent how a
// field changes during an update to a field.

// Null term is a special term that does not have a term key. If e.g. a field is changed from some term keys to null
// term, ModifiedTokens will be termKey1:REMOVE, termKey2:REMOVE..., nullTerm:ADD.
class ModifiedTokens
{
public:
    enum class Operation : uint8_t {
        NONE = 0,
        ADD = 1,
        REMOVE = 2,
    };

private:
    using TermKeyVector = std::vector<uint64_t>;
    using OpVector = std::vector<Operation>;

public:
    ModifiedTokens();
    ModifiedTokens(fieldid_t fieldId);
    void Push(Operation op, uint64_t termKey);

    size_t NonNullTermSize() const { return _termKeys.size(); }
    bool GetNullTermOperation(Operation* op) const;
    bool NullTermExists() const;
    void SetNullTermOperation(Operation op) { _nullTermOp = op; }
    std::pair<uint64_t, Operation> operator[](size_t idx) const;
    fieldid_t FieldId() const { return _fieldId; }
    bool Valid() const { return _fieldId != INVALID_FIELDID; }
    bool Empty() const { return _termKeys.empty() and _nullTermOp == Operation::NONE; }

    void serialize(autil::DataBuffer& buffer) const { Serialize(buffer); }
    void deserialize(autil::DataBuffer& buffer) { Deserialize(buffer); }

    autil::StringView Serialize(autil::DataBuffer& buffer) const;
    void Deserialize(autil::DataBuffer& buffer);
    static bool Equal(const ModifiedTokens& left, const ModifiedTokens& right);

public:
    std::string DebugString() const;

public:
    // Use int64_t to represent term keys instead of uint64_t, in order to use negative numbers to represent delete.
    static ModifiedTokens TEST_CreateModifiedTokens(fieldid_t fieldId, std::vector<int64_t> termKeys);

private:
    fieldid_t _fieldId;
    TermKeyVector _termKeys;
    OpVector _ops;
    Operation _nullTermOp;
};
} // namespace indexlib::document
