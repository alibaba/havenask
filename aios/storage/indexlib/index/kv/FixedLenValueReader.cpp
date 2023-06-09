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
#include "indexlib/index/kv/FixedLenValueReader.h"

#include "autil/mem_pool/Pool.h"
#include "indexlib/file_system/file/FileReader.h"

namespace indexlibv2::index {

FixedLenValueReader::FixedLenValueReader(std::shared_ptr<indexlib::file_system::FileReader> file, uint32_t valueLen)
    : _file(std::move(file))
    , _valueLen(valueLen)
{
}

FixedLenValueReader::~FixedLenValueReader() {}

Status FixedLenValueReader::Read(uint64_t offset, autil::mem_pool::Pool* pool, autil::StringView& data)
{
    assert(pool);
    char* valueBuf = (char*)pool->allocate(_valueLen);
    auto [st, readLen] = _file->Read(valueBuf, _valueLen, offset).StatusWith();
    if (unlikely(!st.IsOK())) {
        return st;
    }
    if (readLen != _valueLen) {
        return Status::Corruption("read fixed value failed");
    }
    data = autil::StringView(valueBuf, _valueLen);
    return Status::OK();
}

} // namespace indexlibv2::index
