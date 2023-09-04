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
#include "indexlib/index/kv/VarLenKVLeafReader.h"

#include "autil/Log.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"
#include "indexlib/index/kv/FixedLenValueReader.h"
#include "indexlib/index/kv/KVCommonDefine.h"
#include "indexlib/index/kv/KVKeyIterator.h"
#include "indexlib/index/kv/VarLenKVSegmentIterator.h"
#include "indexlib/index/kv/VarLenValueReader.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/KVIndexPreference.h"
#include "indexlib/index/kv/config/ValueConfig.h"

using namespace std;

namespace indexlibv2::index {
AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.index, VarLenKVLeafReader);

VarLenKVLeafReader::VarLenKVLeafReader() : _valueBaseAddr(nullptr) {}

VarLenKVLeafReader::~VarLenKVLeafReader() {}

Status VarLenKVLeafReader::Open(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvIndexConfig,
                                const std::shared_ptr<indexlib::file_system::Directory>& indexDirectory)
{
    auto kvDir = indexDirectory->GetDirectory(kvIndexConfig->GetIndexName(), false);
    if (!kvDir) {
        auto status = Status::Corruption("index dir [%s] does not exist in [%s]", kvIndexConfig->GetIndexName().c_str(),
                                         indexDirectory->DebugString().c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }

    auto s = _formatOpts.Load(kvDir);
    if (!s.IsOK()) {
        return s;
    }

    s = _offsetReader.Open(kvDir, kvIndexConfig, _formatOpts);
    if (!s.IsOK()) {
        return s;
    }
    return OpenValue(kvDir, kvIndexConfig);
}

Status VarLenKVLeafReader::OpenValue(const indexlib::file_system::DirectoryPtr& kvDir,
                                     const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvIndexConfig)
{
    indexlib::file_system::ReaderOption readerOption(indexlib::file_system::FSOT_LOAD_CONFIG);
    readerOption.supportCompress = kvIndexConfig->GetIndexPreference().GetValueParam().EnableFileCompress();
    auto [s, fileReader] = kvDir->GetIDirectory()->CreateFileReader(KV_VALUE_FILE_NAME, readerOption).StatusWith();
    if (!s.IsOK()) {
        AUTIL_LOG(ERROR, "load value file from %s failed, error: %s", kvDir->DebugString().c_str(),
                  s.ToString().c_str());
        return s;
    }
    _valueFileReader = std::move(fileReader);
    _valueBaseAddr = (char*)_valueFileReader->GetBaseAddress();

    auto [status, packAttributeConfig] = kvIndexConfig->GetValueConfig()->CreatePackAttributeConfig();
    RETURN_IF_STATUS_ERROR(status, "create pack attribute config fail, indexName[%s]",
                           kvIndexConfig->GetIndexName().c_str());
    _plainFormatEncoder.reset(PackAttributeFormatter::CreatePlainFormatEncoder(packAttributeConfig));
    _fsValueReader.SetPlainFormatEncoder(_plainFormatEncoder.get());
    _fsValueReader.SetFixedValueLen(kvIndexConfig->GetValueConfig()->GetFixedLength());
    return Status::OK();
}

std::unique_ptr<IKVIterator> VarLenKVLeafReader::CreateIterator()
{
    auto keyIterator = _offsetReader.CreateIterator();
    if (!keyIterator) {
        AUTIL_LOG(ERROR, "create key iterator failed for index");
        return nullptr;
    }
    std::unique_ptr<ValueReader> valueReader;
    if (_fsValueReader.IsFixedLen()) {
        valueReader.reset(new FixedLenValueReader(_valueFileReader, _fsValueReader.GetFixedValueLen()));
    } else {
        valueReader.reset(new VarLenValueReader(_valueFileReader));
    }
    return std::make_unique<VarLenKVSegmentIterator>(_formatOpts.IsShortOffset(), std::move(keyIterator),
                                                     std::move(valueReader));
}

size_t VarLenKVLeafReader::EvaluateCurrentMemUsed()
{
    return _offsetReader.EvaluateCurrentMemUsed() + _valueFileReader->EvaluateCurrentMemUsed();
}

} // namespace indexlibv2::index
