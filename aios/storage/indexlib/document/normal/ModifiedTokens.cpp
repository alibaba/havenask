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
#include "indexlib/document/normal/ModifiedTokens.h"

#include "autil/StringUtil.h"

namespace indexlib { namespace document {

ModifiedTokens::ModifiedTokens() : _fieldId(INVALID_FIELDID), _nullTermOp(Operation::NONE) {}
ModifiedTokens::ModifiedTokens(fieldid_t fieldId) : _fieldId(fieldId), _nullTermOp(Operation::NONE) {}

void ModifiedTokens::Push(Operation op, uint64_t termKey)
{
    _termKeys.push_back(termKey);
    _ops.push_back(op);
}

std::pair<uint64_t, ModifiedTokens::Operation> ModifiedTokens::operator[](size_t idx) const
{
    assert(idx < _termKeys.size());
    return std::make_pair(_termKeys[idx], _ops[idx]);
}

autil::StringView ModifiedTokens::Serialize(autil::DataBuffer& buffer) const
{
    buffer.write(_fieldId);
    buffer.write(_termKeys);
    buffer.write(_ops);
    buffer.write(_nullTermOp);
    return autil::StringView(buffer.getData(), buffer.getDataLen());
}

void ModifiedTokens::Deserialize(autil::DataBuffer& buffer)
{
    buffer.read(_fieldId);
    buffer.read(_termKeys);
    buffer.read(_ops);
    buffer.read(_nullTermOp);
}

bool ModifiedTokens::Equal(const ModifiedTokens& left, const ModifiedTokens& right)
{
    if (left.FieldId() != right.FieldId() || left.NonNullTermSize() != right.NonNullTermSize()) {
        return false;
    }

    for (size_t i = 0; i < right.NonNullTermSize(); ++i) {
        if (left[i] != right[i]) {
            return false;
        }
    }
    return left._nullTermOp == right._nullTermOp;
}

bool ModifiedTokens::GetNullTermOperation(Operation* op) const
{
    if (_nullTermOp == Operation::NONE) {
        return false;
    }
    *op = _nullTermOp;
    return true;
}

bool ModifiedTokens::NullTermExists() const { return _nullTermOp != Operation::NONE; }

ModifiedTokens ModifiedTokens::TEST_CreateModifiedTokens(fieldid_t fieldId, std::vector<int64_t> termKeys)
{
    ModifiedTokens tokens(fieldId);
    for (const int64_t key : termKeys) {
        if (key < 0) {
            tokens.Push(ModifiedTokens::Operation::REMOVE, -key);
        } else {
            tokens.Push(ModifiedTokens::Operation::ADD, key);
        }
    }
    return tokens;
}

std::string ModifiedTokens::DebugString() const
{
    std::stringstream ss;
    ss << "fieldId: " << autil::StringUtil::toString(_fieldId) << ", ";
    ss << "term key size: " << autil::StringUtil::toString(_termKeys.size()) << ", ";
    ss << "op size: " << autil::StringUtil::toString(_ops.size());
    return ss.str();
}
}} // namespace indexlib::document
