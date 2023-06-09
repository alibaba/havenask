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
#include "indexlib/index/orc/OrcDiskIndexer.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/orc/OrcConfig.h"
#include "indexlib/index/orc/OrcLeafReader.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, OrcDiskIndexer);

Status OrcDiskIndexer::Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                            const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    auto orcConfig = std::dynamic_pointer_cast<config::OrcConfig>(indexConfig);
    if (orcConfig == nullptr) {
        auto status = Status::InternalError("index: %s, type: %s is not an orc index",
                                            indexConfig->GetIndexName().c_str(), indexConfig->GetIndexType().c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
    auto subDir = indexlib::file_system::IDirectory::ToLegacyDirectory(indexDirectory)
                      ->GetDirectory(orcConfig->GetIndexName(), /*throwExceptionIfNotExist=*/false);
    if (!subDir) {
        auto status =
            Status::InternalError("directory for orc index: %s does not exist", orcConfig->GetIndexName().c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }

    std::shared_ptr<indexlib::file_system::FileReader> fileReader =
        subDir->CreateFileReader(config::OrcConfig::GetOrcFileName(), indexlib::file_system::FSOT_LOAD_CONFIG);
    if (fileReader.get() == nullptr) {
        return Status::InternalError("create file reader fail");
    }
    return PrepareOrcLeafReader(fileReader, orcConfig);
}

Status OrcDiskIndexer::PrepareOrcLeafReader(const std::shared_ptr<indexlib::file_system::FileReader>& fileReader,
                                            const std::shared_ptr<config::OrcConfig>& orcConfig)
{
    auto reader = std::make_shared<OrcLeafReader>();
    auto s = reader->Init(fileReader, orcConfig.get());
    if (s.IsOK()) {
        _reader = reader;
    }
    return s;
}

size_t OrcDiskIndexer::EstimateMemUsed(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                       const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    auto orcConfig = dynamic_cast<const config::OrcConfig*>(indexConfig.get());
    if (orcConfig == nullptr) {
        AUTIL_LOG(ERROR, "index: %s, type %s is not an orc index", indexConfig->GetIndexName().c_str(),
                  indexConfig->GetIndexType().c_str());
        return 0;
    }
    auto subDir = indexlib::file_system::IDirectory::ToLegacyDirectory(indexDirectory)
                      ->GetDirectory(orcConfig->GetIndexName(), /*throwExceptionIfNotExist=*/false);
    if (!subDir) {
        AUTIL_LOG(ERROR, "directory for orc index: %s does not exist", orcConfig->GetIndexName().c_str());
        return 0;
    }
    return subDir->EstimateFileMemoryUse(config::OrcConfig::GetOrcFileName(), indexlib::file_system::FSOT_LOAD_CONFIG);
}

size_t OrcDiskIndexer::EvaluateCurrentMemUsed()
{
    size_t currentMemUse = 0;
    if (_reader) {
        currentMemUse += _reader->EvaluateCurrentMemUsed();
    }
    return currentMemUse;
}

} // namespace indexlibv2::index
