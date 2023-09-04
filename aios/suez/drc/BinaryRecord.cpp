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
#include "suez/drc/BinaryRecord.h"

#include "autil/mem_pool/Pool.h"
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"

namespace suez {

BinaryRecord::BinaryRecord(indexlibv2::index::PackAttributeFormatter *packAttrFormatter,
                           autil::mem_pool::Pool *pool,
                           const autil::StringView &data)
    : _packAttrFormatter(packAttrFormatter), _pool(pool), _data(data) {}

BinaryRecord::~BinaryRecord() {}

size_t BinaryRecord::getFieldCount() const {
    const auto &attrRefs = _packAttrFormatter->GetAttributeReferences();
    return attrRefs.size();
}

autil::StringView BinaryRecord::getFieldName(size_t idx) const {
    const auto &attrRefs = _packAttrFormatter->GetAttributeReferences();
    assert(idx < attrRefs.size());
    const auto &attrRef = attrRefs[idx];
    return attrRef->GetAttrName();
}

bool BinaryRecord::getFieldValue(size_t idx, std::string &value) const {
    const auto &attrRefs = _packAttrFormatter->GetAttributeReferences();
    assert(idx < attrRefs.size());
    const auto &attrRef = attrRefs[idx];
    return attrRef->GetStrValue(_data.data(), value, _pool);
}

} // namespace suez
