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

#include "indexlib/index/kv/FixedLenValueExtractorUtil.h"
#include "indexlib/index/kv/IKVSegmentReader.h"
#include "indexlib/index/kv/KVFormatOptions.h"
#include "indexlib/index/kv/KVTypeId.h"
#include "indexlib/index/kv/KeyReader.h"

namespace indexlibv2::config {
class KVIndexConfig;
}

namespace indexlib::file_system {
class Directory;
class FileReader;
} // namespace indexlib::file_system

namespace indexlibv2::index {

class FixedLenKVLeafReader : public IKVSegmentReader
{
public:
    FixedLenKVLeafReader();
    ~FixedLenKVLeafReader();

public:
    virtual Status Open(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvIndexConfig,
                        const std::shared_ptr<indexlib::file_system::Directory>& indexDirectory);

    FL_LAZY(indexlib::util::Status)
    Get(keytype_t key, autil::StringView& value, uint64_t& ts, autil::mem_pool::Pool* pool = nullptr,
        KVMetricsCollector* collector = nullptr, autil::TimeoutTerminator* timeoutTerminator = nullptr) const override;

    std::unique_ptr<IKVIterator> CreateIterator() override;
    size_t EvaluateCurrentMemUsed() override;

protected:
    KVTypeId _typeId;
    KeyReader _keyReader;
    KVFormatOptions _formatOpts;

private:
    AUTIL_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////////////

inline FL_LAZY(indexlib::util::Status) FixedLenKVLeafReader::Get(keytype_t key, autil::StringView& value, uint64_t& ts,
                                                                 autil::mem_pool::Pool* pool,
                                                                 KVMetricsCollector* collector,
                                                                 autil::TimeoutTerminator* timeoutTerminator) const
{
    autil::StringView offsetStr;
    auto ret = FL_COAWAIT _keyReader.Find(key, offsetStr, ts, collector, pool, timeoutTerminator);
    if (ret == indexlib::util::OK) {
        auto status = FixedLenValueExtractorUtil::ValueExtract((void*)offsetStr.data(), _typeId, pool, value);
        if (!status) {
            AUTIL_LOG(ERROR, "value extract failed, typeId[%s], value len = [%lu]", _typeId.ToString().c_str(),
                      offsetStr.size());
        }
        ret = status ? indexlib::util::OK : indexlib::util::FAIL;
    }
    FL_CORETURN ret;
}

} // namespace indexlibv2::index
