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
#include "indexlib/index/kv/KeyReader.h"

#include "autil/Log.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/kv/FixedLenHashTableCreator.h"
#include "indexlib/index/kv/KVCommonDefine.h"
#include "indexlib/index/kv/KVFormatOptions.h"
#include "indexlib/index/kv/KVKeyIterator.h"
#include "indexlib/index/kv/KVTypeId.h"
#include "indexlib/index/kv/VarLenHashTableCreator.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"

using namespace std;

namespace indexlibv2::index {
AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.index, KeyReader);

KeyReader::KeyReader() {}

KeyReader::~KeyReader() {}

Status KeyReader::Open(const std::shared_ptr<indexlib::file_system::Directory>& kvDir,
                       const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvIndexConfig,
                       const KVFormatOptions& formatOpts)
{
    _typeId = std::make_unique<KVTypeId>();
    *_typeId = MakeKVTypeId(*kvIndexConfig, &formatOpts);
    auto shortOffset = _typeId->shortOffset;
    AUTIL_LOG(INFO, "open kv segment reader with [%s] offset format in path [%s]", (shortOffset ? "short" : "long"),
              kvDir->DebugString().c_str());

    auto result = kvDir->GetIDirectory()->CreateFileReader(KV_KEY_FILE_NAME, indexlib::file_system::FSOT_LOAD_CONFIG);
    if (!result.OK()) {
        return result.Status();
    }
    _keyFileReader = result.Value();

    void* baseAddress = _keyFileReader->GetBaseAddress();
    _inMemory = (baseAddress != nullptr);

    std::unique_ptr<HashTableInfo> hashTableInfo;
    if (_typeId->isVarLen) {
        hashTableInfo = VarLenHashTableCreator::CreateHashTableForReader(*_typeId, !_inMemory);
    } else {
        hashTableInfo = FixedLenHashTableCreator::CreateHashTableForReader(*_typeId, !_inMemory);
    }

    _valueUnpacker = std::move(hashTableInfo->valueUnpacker);
    if (_inMemory) {
        _memoryReader = hashTableInfo->StealHashTable<HashTableBase>();
        if (!_memoryReader) {
            return Status::InternalError("expect type: HashTableBase");
        }
        if (!_memoryReader->MountForRead(baseAddress, _keyFileReader->GetLength())) {
            auto status = Status::Corruption("mount hash table failed, file: %s, length: %lu",
                                             _keyFileReader->DebugString().c_str(), _keyFileReader->GetLength());
            AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
            return status;
        }
    } else {
        _fsReader = hashTableInfo->StealHashTable<HashTableFileReaderBase>();
        if (!_fsReader) {
            auto status = Status::InternalError("expect type: HashTableFileReaderBase");
            AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
            return status;
        }
        if (!_fsReader->Init(kvDir->GetIDirectory(), _keyFileReader)) {
            auto status = Status::Corruption("init hash table file reader failed");
            AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
            return status;
        }
    }
    return Status::OK();
}

std::unique_ptr<KVKeyIterator> KeyReader::CreateIterator() const
{
    std::unique_ptr<HashTableInfo> hashTableInfo;
    if (_typeId->isVarLen) {
        hashTableInfo = VarLenHashTableCreator::CreateHashTableForMerger(*_typeId);
    } else {
        hashTableInfo = FixedLenHashTableCreator::CreateHashTableForMerger(*_typeId);
    }
    auto iterator = std::move(hashTableInfo->hashTableFileIterator);
    if (!iterator) {
        AUTIL_LOG(ERROR, "create hash table file iterator failed");
        return nullptr;
    }
    if (!iterator->Init(_keyFileReader)) {
        AUTIL_LOG(ERROR, "init hash table file iterator failed");
        return nullptr;
    }
    // TODO(xinfei.sxf) remove assert in closed buffer file iterator
    if (_typeId->isVarLen) {
        iterator->SortByValue();
    }
    return std::make_unique<KVKeyIterator>(std::move(iterator), _valueUnpacker.get(), _typeId->isVarLen);
}

size_t KeyReader::EvaluateCurrentMemUsed() { return _keyFileReader->EvaluateCurrentMemUsed(); }

} // namespace indexlibv2::index
