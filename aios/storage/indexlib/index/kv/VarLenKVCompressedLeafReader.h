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

#include "indexlib/file_system/file/CompressFileReader.h"
#include "indexlib/index/kv/VarLenKVLeafReader.h"

namespace indexlib2::config {
class KVIndexConfig;
}

namespace indexlib::file_system {
class Directory;
class CompressFileReader;
} // namespace indexlib::file_system

namespace indexlibv2::index {

class VarLenKVCompressedLeafReader final : public VarLenKVLeafReader
{
public:
    VarLenKVCompressedLeafReader();
    ~VarLenKVCompressedLeafReader();

public:
    Status Open(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvIndexConfig,
                const std::shared_ptr<indexlib::file_system::Directory>& indexDirectory) override;

    FL_LAZY(indexlib::util::Status)
    Get(keytype_t key, autil::StringView& value, uint64_t& ts, autil::mem_pool::Pool* pool = NULL,
        KVMetricsCollector* collector = nullptr,
        autil::TimeoutTerminator* timeoutTerminator = nullptr) const override final;
    size_t EvaluateCurrentMemUsed() override;

private:
    std::shared_ptr<indexlib::file_system::CompressFileReader> _compressedFileReader;
};

inline FL_LAZY(indexlib::util::Status) VarLenKVCompressedLeafReader::Get(
    keytype_t key, autil::StringView& value, uint64_t& ts, autil::mem_pool::Pool* pool, KVMetricsCollector* collector,
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
        indexlib::file_system::CompressFileReader* fileReader = _compressedFileReader->CreateSessionReader(pool);
        assert(fileReader != nullptr);
        indexlib::file_system::CompressFileReaderGuard compressFileGuard(fileReader, pool);
        auto status = FL_COAWAIT _fsValueReader.Read(fileReader, value, offset, pool, collector, timeoutTerminator);
        ret = status ? indexlib::util::OK : indexlib::util::FAIL;
    }
    FL_CORETURN ret;
}

} // namespace indexlibv2::index
