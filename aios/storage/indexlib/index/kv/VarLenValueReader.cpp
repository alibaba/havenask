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
#include "indexlib/index/kv/VarLenValueReader.h"

#include "autil/MultiValueFormatter.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/common/field_format/attribute/MultiValueAttributeFormatter.h"

namespace indexlibv2::index {

VarLenValueReader::VarLenValueReader(std::shared_ptr<indexlib::file_system::FileReader> file) : _file(std::move(file))
{
}

VarLenValueReader::~VarLenValueReader() {}

Status VarLenValueReader::Read(uint64_t offset, autil::mem_pool::Pool* pool, autil::StringView& data)
{
    uint8_t buf[8];
    try {
        size_t readLen = _file->Read(buf, 1, offset).GetOrThrow();
        if (readLen != 1) {
            return Status::Corruption("read first byte failed");
        }
        size_t encodeCountLen = MultiValueAttributeFormatter::GetEncodedCountFromFirstByte(buf[0]);
        readLen = _file->Read(buf + 1, encodeCountLen - 1, offset + 1).GetOrThrow();
        if (readLen != encodeCountLen - 1) {
            return Status::Corruption("read encodeCountLen failed");
        }
        bool isNull = false;
        uint32_t valueCount = MultiValueAttributeFormatter::DecodeCount((const char*)buf, encodeCountLen, isNull);
        (void)isNull;
        size_t valueBytes = valueCount * sizeof(char);
        size_t totalBytes = valueBytes + encodeCountLen;
        char* valueBuf = (char*)pool->allocate(totalBytes);
        memcpy(valueBuf, buf, encodeCountLen);
        readLen = _file->Read(valueBuf + encodeCountLen, valueBytes, offset + encodeCountLen).GetOrThrow();
        if (readLen != valueBytes) {
            return Status::Corruption("read values failed");
        }
        data = autil::StringView(valueBuf, totalBytes);
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::IOError(e.what());
    }
}

} // namespace indexlibv2::index
