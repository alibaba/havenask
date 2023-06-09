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

#include "indexlib/index/common/field_format/pack_attribute/PlainFormatEncoder.h"
#include "indexlib/index/kv/FSValueReader.h"
#include "indexlib/index/kv/IKVSegmentReader.h"
#include "indexlib/index/kv/KVFormatOptions.h"
#include "indexlib/index/kv/KeyReader.h"

namespace indexlibv2::config {
class KVIndexConfig;
}

namespace indexlib::file_system {
class Directory;
class FileReader;
} // namespace indexlib::file_system

namespace indexlibv2::index {

class VarLenKVLeafReader : public IKVSegmentReader
{
public:
    VarLenKVLeafReader();
    ~VarLenKVLeafReader();

public:
    virtual Status Open(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvIndexConfig,
                        const std::shared_ptr<indexlib::file_system::Directory>& indexDirectory);

    FL_LAZY(indexlib::util::Status)
    Get(keytype_t key, autil::StringView& value, uint64_t& ts, autil::mem_pool::Pool* pool,
        KVMetricsCollector* collector, autil::TimeoutTerminator* timeoutTerminator) const override;

    std::unique_ptr<IKVIterator> CreateIterator() override;
    size_t EvaluateCurrentMemUsed() override;

private:
    Status OpenValue(const std::shared_ptr<indexlib::file_system::Directory>& kvDir,
                     const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvIndexConfig);

    // get value
    inline FL_LAZY(bool)
        GetValue(autil::StringView& value, offset_t offset, autil::mem_pool::Pool* pool, KVMetricsCollector* collector,
                 autil::TimeoutTerminator* timeoutTerminator) const __ALWAYS_INLINE;

    inline bool GetValueFromMemory(autil::StringView& value, offset_t offset,
                                   autil::mem_pool::Pool* pool) const __ALWAYS_INLINE;

    inline bool InMemory() const { return _valueBaseAddr != nullptr; }

protected:
    KVFormatOptions _formatOpts;
    KeyReader _offsetReader;
    char* _valueBaseAddr;                                                // memory reader
    FSValueReader _fsValueReader;                                        // fs reader
    std::shared_ptr<indexlib::file_system::FileReader> _valueFileReader; // value file
    std::shared_ptr<PlainFormatEncoder> _plainFormatEncoder;
};

///////////////////////////////////////////////////////////////////////
inline bool VarLenKVLeafReader::GetValueFromMemory(autil::StringView& value, offset_t offset,
                                                   autil::mem_pool::Pool* pool) const
{
    if (_fsValueReader.IsFixedLen()) {
        assert(!_plainFormatEncoder);
        autil::MultiChar multiChar(_valueBaseAddr + offset, _fsValueReader.GetFixedValueLen());
        value = {multiChar.data(), multiChar.size()};
        return true;
    }

    autil::MultiChar multiChar(_valueBaseAddr + offset);
    value = {multiChar.data(), multiChar.size()};
    if (_plainFormatEncoder) {
        return _plainFormatEncoder->Decode(value, pool, value);
    }
    return true;
}

inline FL_LAZY(bool) VarLenKVLeafReader::GetValue(autil::StringView& value, offset_t offset,
                                                  autil::mem_pool::Pool* pool, KVMetricsCollector* collector,
                                                  autil::TimeoutTerminator* timeoutTerminator) const
{
    if (InMemory()) {
        FL_CORETURN GetValueFromMemory(value, offset, pool);
    }
    FL_CORETURN FL_COAWAIT _fsValueReader.Read(_valueFileReader.get(), value, offset, pool, collector,
                                               timeoutTerminator);
}

inline FL_LAZY(indexlib::util::Status) VarLenKVLeafReader::Get(keytype_t key, autil::StringView& value, uint64_t& ts,
                                                               autil::mem_pool::Pool* pool,
                                                               KVMetricsCollector* collector,
                                                               autil::TimeoutTerminator* timeoutTerminator) const
{
    autil::StringView offsetStr;
    auto ret = FL_COAWAIT _offsetReader.Find(key, offsetStr, ts, collector, pool, timeoutTerminator);
    if (ret == indexlib::util::OK) {
        offset_t offset = 0;
        if (_formatOpts.IsShortOffset()) {
            offset = *(short_offset_t*)(offsetStr.data());
        } else {
            offset = *(offset_t*)(offsetStr.data());
        }
        auto status = FL_COAWAIT GetValue(value, offset, pool, collector, timeoutTerminator);
        ret = status ? indexlib::util::OK : indexlib::util::FAIL;
    }
    FL_CORETURN ret;
}

} // namespace indexlibv2::index
