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

#include "indexlib/file_system/FSResult.h"
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/inverted_index/Types.h"
#include "indexlib/index/inverted_index/config/DictionaryConfig.h"
#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"

namespace indexlib::file_system {
class ArchiveFolder;
class IDirectory;
class FileReader;
} // namespace indexlib::file_system

namespace indexlib::config {

class HighFreqVocabularyCreator
{
public:
    HighFreqVocabularyCreator();
    ~HighFreqVocabularyCreator();

public:
    static std::shared_ptr<HighFrequencyVocabulary>
    CreateVocabulary(const std::string& indexName, InvertedIndexType indexType,
                     const std::shared_ptr<DictionaryConfig>& dictConfig, const std::string& nullLiteralString,
                     const util::KeyValueMap& dictHashParams);

    static file_system::FSResult<std::shared_ptr<HighFrequencyVocabulary>>
    LoadAdaptiveVocabulary(const std::shared_ptr<file_system::ArchiveFolder>& adaptiveHighFrequeryFolder,
                           const std::string& indexName, InvertedIndexType indexType,
                           const std::string& nullLiteralString, const std::shared_ptr<DictionaryConfig>& dictConfig,
                           const util::KeyValueMap& dictHashParams);

    static file_system::FSResult<std::shared_ptr<HighFrequencyVocabulary>>
    LoadAdaptiveVocabulary(const std::shared_ptr<file_system::IDirectory>& adaptiveDictDir,
                           const std::string& indexName, InvertedIndexType indexType,
                           const std::string& nullLiteralString, const std::shared_ptr<DictionaryConfig>& dictConfig,
                           const util::KeyValueMap& dictHashParams);

    // for dump
    static std::shared_ptr<HighFrequencyVocabulary>
    CreateAdaptiveVocabulary(const std::string& indexName, InvertedIndexType indexType,
                             const std::string& nullLiteralString, const std::shared_ptr<DictionaryConfig>& dictConfig,
                             const util::KeyValueMap& dictHashParams);

private:
    static std::string GetAdaptiveHFDictName(const std::string& indexName,
                                             const std::shared_ptr<DictionaryConfig>& dictConfig);
    static file_system::FSResult<bool>
    GetAdaptiveHFTermSigns(const std::shared_ptr<file_system::FileReader>& archiveFile,
                           std::vector<index::DictKeyInfo>& termSigns);
    static void GetAdaptiveHFTermSigns(const std::string& fileContent, std::vector<index::DictKeyInfo>& termSigns);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::config
