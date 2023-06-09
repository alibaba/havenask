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
#include "indexlib/legacy/index_base/SchemaAdapter.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/archive/ArchiveFolder.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/inverted_index/config/HighFreqVocabularyCreator.h"

using namespace indexlib::file_system;
using namespace indexlib::config;

namespace indexlib::legacy::index_base {
AUTIL_LOG_SETUP(indexlib.index_base, SchemaAdapter);

void SchemaAdapter::LoadAdaptiveBitmapTerms(const DirectoryPtr& rootDir, IndexPartitionSchema* schema)
{
    if (schema->GetTableType() != tt_index) {
        return;
    }

    std::vector<IndexConfigPtr> adaptiveDictIndexConfigs;
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    auto indexConfigs = indexSchema->CreateIterator(true);
    auto iter = indexConfigs->Begin();
    for (; iter != indexConfigs->End(); iter++) {
        const IndexConfigPtr& indexConfig = *iter;
        if (indexConfig->GetShardingIndexConfigs().size() > 0) {
            // if is sharding index, don't read HFV for original index name
            continue;
        }
        if (indexConfig->GetAdaptiveDictionaryConfig()) {
            adaptiveDictIndexConfigs.push_back(indexConfig);
        }
    }

    if (adaptiveDictIndexConfigs.empty()) {
        return;
    }
    auto [ec, isExist] = FslibWrapper::IsExist(rootDir->GetPhysicalPath("") + "/" + ADAPTIVE_DICT_DIR_NAME);
    if (ec != FSEC_OK) {
        AUTIL_LOG(ERROR, "FslibWrapper::IsExist adaptive dict dir failed, ec=%d", ec);
        return;
    }
    if (!isExist) {
        return;
    }
    auto fs = rootDir->GetIDirectory()->GetFileSystem();
    if (fs != nullptr) {
        ec = fs->MountDir(/*physicalRoot=*/rootDir->GetPhysicalPath(""),
                          /*physicalPath=*/ADAPTIVE_DICT_DIR_NAME,
                          /*logicalPath=*/ADAPTIVE_DICT_DIR_NAME,
                          /*MountDirOption=*/indexlib::file_system::FSMT_READ_WRITE,
                          /*enableLazyMount=*/false)
                 .Code();
    } else {
        AUTIL_LOG(ERROR, "fs is null, probably not using file system. This should not occur in prod environment.");
    }
    if (ec != FSEC_OK) {
        AUTIL_LOG(ERROR, "mount adaptive dict dir failed, ec=%d", ec);
        return;
    }
    std::shared_ptr<Directory> directory = rootDir->GetDirectory(ADAPTIVE_DICT_DIR_NAME, false);
    if (!directory) {
        return;
    }
    // adaptiveBitmapDir might be in archive file or package format.
    const std::string PACKAGE_META_FILE_NAME = std::string(indexlib::file_system::PACKAGE_FILE_PREFIX) +
                                               std::string(indexlib::file_system::PACKAGE_FILE_META_SUFFIX);
    bool packageExists = directory->IsExist(PACKAGE_META_FILE_NAME);
    if (packageExists) {
        for (size_t i = 0; i < adaptiveDictIndexConfigs.size(); i++) {
            auto indexConfig = adaptiveDictIndexConfigs[i];
            auto [status, vol] = indexlib::config::HighFreqVocabularyCreator::LoadAdaptiveVocabulary(
                                     directory->GetIDirectory(), indexConfig->GetIndexName(),
                                     indexConfig->GetInvertedIndexType(), indexConfig->GetNullTermLiteralString(),
                                     indexConfig->GetDictConfig(), indexConfig->GetDictHashParams())
                                     .StatusWith();
            if (!status.IsOK()) {
                AUTIL_LOG(ERROR, "load adaptive vocabulary failed, [%s]", status.ToString().c_str());
                return;
            }
            if (vol != nullptr) {
                indexConfig->SetHighFreqVocabulary(vol);
            }
        }
        return;
    }

    ArchiveFolderPtr adaptiveDictFolder = directory->CreateArchiveFolder(false, "");
    for (size_t i = 0; i < adaptiveDictIndexConfigs.size(); i++) {
        IndexConfigPtr indexConfig = adaptiveDictIndexConfigs[i];
        std::shared_ptr<HighFrequencyVocabulary> vol =
            HighFreqVocabularyCreator::LoadAdaptiveVocabulary(
                adaptiveDictFolder, indexConfig->GetIndexName(), indexConfig->GetInvertedIndexType(),
                indexConfig->GetNullTermLiteralString(), indexConfig->GetDictConfig(), indexConfig->GetDictHashParams())
                .GetOrThrow();
        if (vol) {
            indexConfig->SetHighFreqVocabulary(vol);
        }
    }
    adaptiveDictFolder->Close().GetOrThrow();
}

} // namespace indexlib::legacy::index_base
