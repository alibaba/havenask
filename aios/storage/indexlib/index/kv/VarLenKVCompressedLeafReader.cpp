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
#include "indexlib/index/kv/VarLenKVCompressedLeafReader.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/KVIndexPreference.h"

namespace indexlibv2::index {

VarLenKVCompressedLeafReader::VarLenKVCompressedLeafReader() {}

VarLenKVCompressedLeafReader::~VarLenKVCompressedLeafReader() {}

Status VarLenKVCompressedLeafReader::Open(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvIndexConfig,
                                          const std::shared_ptr<indexlib::file_system::Directory>& indexDirectory)
{
    auto s = VarLenKVLeafReader::Open(kvIndexConfig, indexDirectory);
    if (!s.IsOK()) {
        return s;
    }
    _compressedFileReader = std::dynamic_pointer_cast<indexlib::file_system::CompressFileReader>(_valueFileReader);
    if (!_compressedFileReader) {
        return Status::InternalError("expect an instance of CompressFileReader");
    }

    const auto& valueParam = kvIndexConfig->GetIndexPreference().GetValueParam();
    const auto& compressInfo = _compressedFileReader->GetCompressInfo();
    if (valueParam.GetFileCompressType() != compressInfo->compressorName) {
        return Status::Corruption("file compressor not equal, in schema: %s, in index %s",
                                  valueParam.GetFileCompressType().c_str(), compressInfo->compressorName.c_str());
    }
    return Status::OK();
}

size_t VarLenKVCompressedLeafReader::EvaluateCurrentMemUsed()
{
    size_t memoryUsage = VarLenKVLeafReader::EvaluateCurrentMemUsed();
    const auto& addressMapper = _compressedFileReader->GetCompressFileAddressMapper();
    if (addressMapper) {
        memoryUsage += addressMapper->EvaluateCurrentMemUsed();
    }
    return memoryUsage;
}

} // namespace indexlibv2::index
