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
#include "indexlib/index/kv/FixedLenKVLeafReader.h"

#include "autil/Log.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/kv/FixedLenKVSegmentIterator.h"
#include "indexlib/index/kv/KVCommonDefine.h"
#include "indexlib/index/kv/KVKeyIterator.h"
#include "indexlib/index/kv/KVTypeId.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"

namespace indexlibv2::index {

AUTIL_LOG_SETUP(indexlib.index, FixedLenKVLeafReader);

FixedLenKVLeafReader::FixedLenKVLeafReader() {}

FixedLenKVLeafReader::~FixedLenKVLeafReader() {}

Status FixedLenKVLeafReader::Open(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvIndexConfig,
                                  const std::shared_ptr<indexlib::file_system::Directory>& indexDirectory)
{
    auto kvDir = indexDirectory->GetDirectory(kvIndexConfig->GetIndexName(), false);
    if (!kvDir) {
        return Status::Corruption("index dir: %s does not exist in %s.", kvIndexConfig->GetIndexName().c_str(),
                                  indexDirectory->DebugString().c_str());
    }

    auto status = _formatOpts.Load(kvDir);
    RETURN_IF_STATUS_ERROR(status, "load format options failed, path[%s]", kvDir->DebugString().c_str());

    _typeId = MakeKVTypeId(*kvIndexConfig.get(), &_formatOpts);

    return _keyReader.Open(kvDir, kvIndexConfig, _formatOpts);
}

std::unique_ptr<IKVIterator> FixedLenKVLeafReader::CreateIterator()
{
    auto keyIterator = _keyReader.CreateIterator();
    if (!keyIterator) {
        AUTIL_LOG(ERROR, "create key iterator failed for index");
        return nullptr;
    }
    return std::make_unique<FixedLenKVSegmentIterator>(_typeId, std::move(keyIterator));
}

size_t FixedLenKVLeafReader::EvaluateCurrentMemUsed() { return _keyReader.EvaluateCurrentMemUsed(); }

} // namespace indexlibv2::index
