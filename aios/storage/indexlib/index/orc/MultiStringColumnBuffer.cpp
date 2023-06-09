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
#include "indexlib/index/orc/MultiStringColumnBuffer.h"

namespace indexlibv2::index {

MultiStringColumnBuffer::MultiStringColumnBuffer(const config::FieldConfig* field, size_t batchSize)
    : ColumnBuffer(field, batchSize)
    , _strCount(0)
    , _totalLength(0)
{
}

Status MultiStringColumnBuffer::Init(orc::ColumnVectorBatch* batch)
{
    if (_batchSize <= 0) {
        return Status::InvalidArgs("batch size should be larger than 0");
    }
    _parent = dynamic_cast<orc::ListVectorBatch*>(batch);
    if (_parent == nullptr) {
        return Status::InvalidArgs("fail to dynamic_cast ColumnVectorBatch to ListVectorBatch");
    }
    _child = dynamic_cast<orc::StringVectorBatch*>(_parent->elements.get());
    if (_child == nullptr) {
        return Status::InvalidArgs("fail to dynamic_cast ColumnVectorBatch to StringVectorBatch");
    }
    _parent->offsets[0] = 0;
    _convertor = std::make_unique<MultiStringAttributeConvertor>(/*needHash=*/false, _field->GetFieldName(), false,
                                                                 INDEXLIB_MULTI_VALUE_SEPARATOR_STR);
    assert(_convertor);
    return Status::OK();
}

void MultiStringColumnBuffer::Append(const autil::StringView& field, uint64_t rowIdx)
{
    auto attrMeta = _convertor->Decode(field);
    autil::MultiString multiStr(attrMeta.data.data());
    if (multiStr.size() > 0) {
        _parent->notNull[rowIdx] = true;
    } else {
        _parent->notNull[rowIdx] = false;
        _parent->hasNulls = true;
    }
    auto& offsets = _parent->offsets;
    offsets[rowIdx + 1] = offsets[rowIdx];
    auto iter = multiStr.createIterator();
    while (iter.hasNext()) {
        auto multiChar = iter.next();
        size_t strLen = multiChar.size();
        MaybeResizeChild();
        if (strLen > 0) {
            const char* str = multiChar.data();
            _child->blob.append(str, strLen);
            _totalLength += strLen;
        }
        _child->notNull[_strCount] = true;
        _child->length[_strCount] = strLen;
        _strCount++;
    }
    offsets[rowIdx + 1] += multiStr.size();
}

} // namespace indexlibv2::index
