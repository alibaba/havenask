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

#include "indexlib/base/Define.h"
#include "indexlib/base/Status.h"
#include "indexlib/index/common/hash_table/HashTableBase.h"
#include "indexlib/index/common/hash_table/HashTableFileReaderBase.h"
#include "indexlib/index/kv/KVCommonDefine.h"
#include "indexlib/index/kv/KVFormatOptions.h"
#include "indexlib/index/kv/KVMetricsCollector.h"
#include "indexlib/index/kv/KVTypeId.h"

namespace indexlibv2::config {
class KVIndexConfig;
}

namespace indexlib::file_system {
class Directory;
class FileReader;
} // namespace indexlib::file_system

namespace indexlibv2::index {
class KVKeyIterator;

class KeyReader
{
public:
    KeyReader();
    ~KeyReader();

public:
    Status Open(const std::shared_ptr<indexlib::file_system::Directory>& kvDir,
                const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvIndexConfig,
                const KVFormatOptions& formatOpts);

    inline FL_LAZY(indexlib::util::Status)
        Find(keytype_t key, autil::StringView& value, uint64_t& ts, KVMetricsCollector* collector,
             autil::mem_pool::Pool* pool, autil::TimeoutTerminator* timeoutTerminator) const __ALWAYS_INLINE;

    std::unique_ptr<KVKeyIterator> CreateIterator() const; // for merge
    size_t EvaluateCurrentMemUsed();

private:
    bool _inMemory = false;
    std::unique_ptr<ValueUnpacker> _valueUnpacker;
    std::unique_ptr<HashTableBase> _memoryReader;
    std::unique_ptr<HashTableFileReaderBase> _fsReader;
    std::shared_ptr<indexlib::file_system::FileReader> _keyFileReader;
    std::unique_ptr<KVTypeId> _typeId; // used for create iterator
};

///////////////////////////////////////////////////////////////
inline FL_LAZY(indexlib::util::Status) KeyReader::Find(keytype_t key, autil::StringView& value, uint64_t& ts,
                                                       KVMetricsCollector* collector, autil::mem_pool::Pool* pool,
                                                       autil::TimeoutTerminator* timeoutTerminator) const
{
    autil::StringView tmpValue;
    indexlib::util::Status status;
    if (_inMemory) {
        status = _memoryReader->Find(key, tmpValue);
    } else {
        // TODO(xinfei.sxf) add timeout
        indexlib::util::BlockAccessCounter* blockCounter = collector ? collector->GetBlockCounter() : nullptr;
        status = FL_COAWAIT _fsReader->Find(key, tmpValue, blockCounter, pool, timeoutTerminator);
    }
    if (status == indexlib::util::NOT_FOUND) {
        FL_CORETURN status;
    }
    if (status != indexlib::util::OK && status != indexlib::util::DELETED) {
        FL_CORETURN status;
    }
    // TODO(xinfei.sxf) we don't need to unpack value in fail case
    _valueUnpacker->Unpack(tmpValue, ts, value);
    FL_CORETURN status;
}

} // namespace indexlibv2::index
