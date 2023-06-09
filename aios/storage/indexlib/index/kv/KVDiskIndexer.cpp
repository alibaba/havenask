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
#include "indexlib/index/kv/KVDiskIndexer.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/index/kv/FixedLenKVLeafReader.h"
#include "indexlib/index/kv/KVTypeId.h"
#include "indexlib/index/kv/VarLenKVCompressedLeafReader.h"
#include "indexlib/index/kv/VarLenKVLeafReader.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"

using namespace std;
using namespace indexlib;
using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlib::file_system;

namespace indexlibv2::index {
AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.index, KVDiskIndexer);

Status KVDiskIndexer::Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                           const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    _kvIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(indexConfig);
    _indexDirectory = IDirectory::ToLegacyDirectory(indexDirectory);
    auto typeId = MakeKVTypeId(*_kvIndexConfig, nullptr);
    if (typeId.isVarLen) {
        if (!typeId.fileCompress) {
            auto reader = std::make_shared<VarLenKVLeafReader>();
            auto s = reader->Open(_kvIndexConfig, _indexDirectory);
            if (!s.IsOK()) {
                return s;
            }
            _reader = std::move(reader);
        } else {
            auto reader = std::make_shared<VarLenKVCompressedLeafReader>();
            auto s = reader->Open(_kvIndexConfig, _indexDirectory);
            if (!s.IsOK()) {
                return s;
            }
            _reader = std::move(reader);
        }
    } else {
        auto reader = std::make_shared<FixedLenKVLeafReader>();
        auto s = reader->Open(_kvIndexConfig, _indexDirectory);
        if (!s.IsOK()) {
            return s;
        }
        _reader = std::move(reader);
    }
    return Status::OK();
}

size_t KVDiskIndexer::EstimateMemUsed(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                      const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    auto kvIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(indexConfig);
    if (!kvIndexConfig) {
        AUTIL_LOG(ERROR, "index: %s, type %s is not an kv index", indexConfig->GetIndexName().c_str(),
                  indexConfig->GetIndexType().c_str());
        return 0;
    }

    auto kvDir = IDirectory::ToLegacyDirectory(indexDirectory)->GetDirectory(kvIndexConfig->GetIndexName(), false);
    if (!kvDir) {
        AUTIL_LOG(ERROR, "directory for kv index: %s does not exist", kvIndexConfig->GetIndexName().c_str());
        return 0;
    }
    size_t keyMemory = kvDir->EstimateFileMemoryUse(KV_KEY_FILE_NAME, indexlib::file_system::FSOT_LOAD_CONFIG);
    size_t valueMemory = kvDir->EstimateFileMemoryUse(KV_VALUE_FILE_NAME, indexlib::file_system::FSOT_LOAD_CONFIG);

    size_t compressMapperMemory = 0;
    auto typeId = MakeKVTypeId(*kvIndexConfig, nullptr);
    if (typeId.isVarLen && typeId.fileCompress) {
        compressMapperMemory = CompressFileAddressMapper::EstimateMemUsed(kvDir->GetIDirectory(), KV_VALUE_FILE_NAME,
                                                                          indexlib::file_system::FSOT_LOAD_CONFIG);
    }
    return keyMemory + valueMemory + compressMapperMemory;
}

size_t KVDiskIndexer::EvaluateCurrentMemUsed() { return _reader ? _reader->EvaluateCurrentMemUsed() : 0; }

} // namespace indexlibv2::index
