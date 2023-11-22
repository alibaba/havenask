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
#include "indexlib/index/primary_key/PrimaryKeyIndexFields.h"

#include "indexlib/index/primary_key/Common.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, PrimaryKeyIndexFields);

PrimaryKeyIndexFields::PrimaryKeyIndexFields() {}

PrimaryKeyIndexFields::~PrimaryKeyIndexFields() {}

void PrimaryKeyIndexFields::serialize(autil::DataBuffer& dataBuffer) const
{
    dataBuffer.write(SERIALIZE_VERSION);
    dataBuffer.write(_primaryKey);
}
void PrimaryKeyIndexFields::deserialize(autil::DataBuffer& dataBuffer)
{
    uint32_t serializeVersion = 0;
    dataBuffer.read(serializeVersion);
    assert(serializeVersion == SERIALIZE_VERSION);
    dataBuffer.read(_primaryKey);
}

autil::StringView PrimaryKeyIndexFields::GetIndexType() const { return PRIMARY_KEY_INDEX_TYPE_STR; }
size_t PrimaryKeyIndexFields::EstimateMemory() const { return _primaryKey.size(); }

void PrimaryKeyIndexFields::SetPrimaryKey(const std::string& primaryKey) { _primaryKey = primaryKey; }
const std::string& PrimaryKeyIndexFields::GetPrimaryKey() const { return _primaryKey; }

} // namespace indexlibv2::index
