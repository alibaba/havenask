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
#include "autil/StringView.h"
#include "autil/mem_pool/Pool.h"
#include "future_lite/CoroInterface.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/common/field_format/attribute/MultiValueAttributeFormatter.h"
#include "indexlib/index/common/field_format/pack_attribute/PlainFormatEncoder.h"
#include "indexlib/index/kv/KVMetricsCollector.h"

namespace indexlibv2::index {

class FSValueReader final
{
public:
    FSValueReader(int32_t fixedValueLen = -1);
    ~FSValueReader();

public:
    inline FL_LAZY(bool) Read(indexlib::file_system::FileReader* reader, autil::StringView& value, offset_t offset,
                              autil::mem_pool::Pool* pool, KVMetricsCollector* collector,
                              autil::TimeoutTerminator* timeoutTerminator) const __attribute__((always_inline));

public:
    void SetFixedValueLen(int32_t fixedValueLen);
    void SetPlainFormatEncoder(PlainFormatEncoder* encoder) { _plainFormatEncoder = encoder; }
    inline int32_t GetFixedValueLen() const { return _fixedValueLen; }
    inline bool IsFixedLen() const { return _fixedValueLen > 0; }

private:
    int32_t _fixedValueLen;
    PlainFormatEncoder* _plainFormatEncoder = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

FL_LAZY(bool)
FSValueReader::Read(indexlib::file_system::FileReader* reader, autil::StringView& value, offset_t offset,
                    autil::mem_pool::Pool* pool, KVMetricsCollector* collector,
                    autil::TimeoutTerminator* timeoutTerminator) const
{
    size_t encodeCountLen = 0;
    size_t itemLen = 0;

    indexlib::file_system::ReadOption option;
    option.blockCounter = collector ? collector->GetBlockCounter() : nullptr;
    option.advice = indexlib::file_system::IO_ADVICE_LOW_LATENCY;
    option.timeoutTerminator = timeoutTerminator;
    if (IsFixedLen()) {
        itemLen = _fixedValueLen;
    } else {
        uint8_t lenBuffer[8];
        auto ret = FL_COAWAIT reader->ReadAsyncCoro(lenBuffer, 1, offset, option);
        if (!ret.OK() || 1 != ret.Value()) {
            AUTIL_LOG(ERROR, "read encodeCount first bytes failed from file[%s]", reader->DebugString().c_str());
            FL_CORETURN false;
        }

        encodeCountLen = MultiValueAttributeFormatter::GetEncodedCountFromFirstByte(lenBuffer[0]);
        assert(encodeCountLen <= sizeof(lenBuffer));
        ret = FL_COAWAIT reader->ReadAsyncCoro(lenBuffer + 1, encodeCountLen - 1, offset + 1, option);
        if (!ret.OK() || encodeCountLen - 1 != ret.Value()) {
            AUTIL_LOG(ERROR, "read encodeCount from file[%s]", reader->DebugString().c_str());
            FL_CORETURN false;
        }

        bool isNull = false;
        uint32_t itemCount = MultiValueAttributeFormatter::DecodeCount((const char*)lenBuffer, encodeCountLen, isNull);
        assert(!isNull);
        itemLen = itemCount * sizeof(char);
    }
    char* buffer = (char*)pool->allocate(itemLen);
    auto ret = FL_COAWAIT reader->ReadAsyncCoro(buffer, itemLen, offset + encodeCountLen, option);
    if (!ret.OK() || itemLen != ret.Value()) {
        AUTIL_LOG(ERROR, "read value from file[%s]", reader->DebugString().c_str());
        FL_CORETURN false;
    }
    value = {buffer, itemLen};
    if (_plainFormatEncoder && !_plainFormatEncoder->Decode(value, pool, value)) {
        AUTIL_LOG(ERROR, "decode plain format from file[%s] fail.", reader->DebugString().c_str());
        FL_CORETURN false;
    }
    FL_CORETURN true;
}

} // namespace indexlibv2::index
