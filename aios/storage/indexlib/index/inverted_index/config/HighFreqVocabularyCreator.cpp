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
#include "indexlib/index/inverted_index/config/HighFreqVocabularyCreator.h"

#include "autil/Log.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/archive/ArchiveFile.h"
#include "indexlib/file_system/archive/ArchiveFolder.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace indexlib::file_system;
using namespace indexlib::util;
using namespace indexlib::index;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, HighFreqVocabularyCreator);

HighFreqVocabularyCreator::HighFreqVocabularyCreator() {}

HighFreqVocabularyCreator::~HighFreqVocabularyCreator() {}

std::shared_ptr<HighFrequencyVocabulary>
HighFreqVocabularyCreator::CreateVocabulary(const string& indexName, InvertedIndexType indexType,
                                            const std::shared_ptr<DictionaryConfig>& dictConfig,
                                            const string& nullLiteralString, const util::KeyValueMap& dictHashParams)
{
    if (!dictConfig) {
        return std::shared_ptr<HighFrequencyVocabulary>();
    }
    std::shared_ptr<HighFrequencyVocabulary> volcabulary(new HighFrequencyVocabulary(nullLiteralString));
    volcabulary->Init(indexName, indexType, dictConfig->GetContent(), dictHashParams);
    return volcabulary;
}

// check index need create adaptive vocabulary
FSResult<std::shared_ptr<HighFrequencyVocabulary>> HighFreqVocabularyCreator::LoadAdaptiveVocabulary(
    const ArchiveFolderPtr& adaptiveHighFrequeryFolder, const string& indexName, InvertedIndexType indexType,
    const string& nullLiteralString, const std::shared_ptr<DictionaryConfig>& dictConfig,
    const util::KeyValueMap& dictHashParams)
{
    std::shared_ptr<HighFrequencyVocabulary> volcabulary;
    if (!adaptiveHighFrequeryFolder) {
        return FSResult(FSEC_OK, std::move(volcabulary));
    }

    string volcabularyName = GetAdaptiveHFDictName(indexName, dictConfig);
    string dictContent = "";
    if (dictConfig) {
        dictContent = dictConfig->GetContent();
    }
    volcabulary.reset(new HighFrequencyVocabulary(nullLiteralString));
    volcabulary->Init(volcabularyName, indexType, dictContent, dictHashParams);

    auto [ec, adaptiveHighFrequeryFile] = adaptiveHighFrequeryFolder->CreateFileReader(volcabularyName).CodeWith();
    RETURN2_IF_FS_ERROR(ec, std::shared_ptr<HighFrequencyVocabulary>(), "create file reader[%s] failed",
                        volcabularyName.c_str());
    vector<DictKeyInfo> termSigns;
    auto [ec2, ret] = GetAdaptiveHFTermSigns(adaptiveHighFrequeryFile, termSigns);
    RETURN2_IF_FS_ERROR(ec2, std::shared_ptr<HighFrequencyVocabulary>(),
                        "get adaptive hf_frequency bitmap terms from [%s] failed!", volcabularyName.c_str());
    if (!ret) {
        AUTIL_LOG(WARN, "get adaptive hf_frequency bitmap terms from [%s] failed!", volcabularyName.c_str());
        return FSResult(FSEC_OK, std::shared_ptr<HighFrequencyVocabulary>());
    }
    for (size_t i = 0; i < termSigns.size(); ++i) {
        volcabulary->AddKey(termSigns[i]);
    }
    RETURN2_IF_FS_ERROR(adaptiveHighFrequeryFile->Close(), std::shared_ptr<HighFrequencyVocabulary>(),
                        "close file[%s] failed", adaptiveHighFrequeryFile->DebugString().c_str());
    return FSResult(FSEC_OK, volcabulary);
}

FSResult<std::shared_ptr<HighFrequencyVocabulary>> HighFreqVocabularyCreator::LoadAdaptiveVocabulary(
    const std::shared_ptr<file_system::IDirectory>& adaptiveDictDir, const string& indexName,
    InvertedIndexType indexType, const string& nullLiteralString, const std::shared_ptr<DictionaryConfig>& dictConfig,
    const util::KeyValueMap& dictHashParams)
{
    std::shared_ptr<HighFrequencyVocabulary> volcabulary;

    volcabulary.reset(new HighFrequencyVocabulary(nullLiteralString));
    string volcabularyName = GetAdaptiveHFDictName(indexName, dictConfig);
    string dictContent = "";
    if (dictConfig) {
        dictContent = dictConfig->GetContent();
    }
    volcabulary->Init(volcabularyName, indexType, dictContent, dictHashParams);

    string fileContent;
    auto ec = adaptiveDictDir->Load(volcabularyName, ReaderOption::PutIntoCache(FSOT_MEM), fileContent).Code();
    if (ec == FSEC_NOENT) {
        AUTIL_LOG(WARN, "get adaptive hf_frequency bitmap terms from [%s] in [%s] failed!", volcabularyName.c_str(),
                  adaptiveDictDir->DebugString().c_str());
        return FSResult(FSEC_OK, std::shared_ptr<HighFrequencyVocabulary>());
    }
    RETURN2_IF_FS_ERROR(ec, std::shared_ptr<HighFrequencyVocabulary>(), "load [%s] from [%s] failed",
                        volcabularyName.c_str(), adaptiveDictDir->DebugString().c_str());

    vector<DictKeyInfo> termSigns;
    GetAdaptiveHFTermSigns(fileContent, termSigns);
    for (size_t i = 0; i < termSigns.size(); ++i) {
        volcabulary->AddKey(termSigns[i]);
    }
    return FSResult(FSEC_OK, volcabulary);
}

std::shared_ptr<HighFrequencyVocabulary> HighFreqVocabularyCreator::CreateAdaptiveVocabulary(
    const string& indexName, InvertedIndexType indexType, const string& nullLiteralString,
    const std::shared_ptr<DictionaryConfig>& dictConfig, const util::KeyValueMap& dictHashParams)
{
    std::shared_ptr<HighFrequencyVocabulary> volcabulary(new HighFrequencyVocabulary(nullLiteralString));
    string volcabularyName = GetAdaptiveHFDictName(indexName, dictConfig);
    string dictContent = "";
    if (dictConfig) {
        dictContent = dictConfig->GetContent();
    }
    volcabulary->Init(volcabularyName, indexType, dictContent, dictHashParams);
    return volcabulary;
}

FSResult<bool> HighFreqVocabularyCreator::GetAdaptiveHFTermSigns(const FileReaderPtr& archiveFile,
                                                                 vector<DictKeyInfo>& termSigns)
{
    if (!archiveFile) {
        return FSResult(FSEC_OK, false);
    }

    string fileContent;
    auto len = archiveFile->GetLength();
    std::unique_ptr<std::vector<char>> buf(new vector<char>(len));
    auto [ec, readLen] = archiveFile->Read(buf->data(), len).CodeWith();
    RETURN2_IF_FS_ERROR(ec, false, "read [%s] failed", archiveFile->DebugString().c_str());
    if (readLen != len) {
        AUTIL_LOG(ERROR, "read [%s] failed, readLen[%lu] != len[%lu]", archiveFile->DebugString().c_str(), readLen,
                  len);
        return FSResult(FSEC_ERROR, false);
    }
    fileContent.assign(buf->data(), buf->size());

    StringTokenizer st(fileContent, "\n", StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    StringTokenizer::Iterator it = st.begin();
    while (it != st.end()) {
        DictKeyInfo key;
        key.FromString((*it));
        termSigns.push_back(key);
        ++it;
    }
    return FSResult(FSEC_OK, true);
}

void HighFreqVocabularyCreator::GetAdaptiveHFTermSigns(const string& fileContent, vector<DictKeyInfo>& termSigns)
{
    StringTokenizer st(fileContent, "\n", StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);

    StringTokenizer::Iterator it = st.begin();
    while (it != st.end()) {
        DictKeyInfo key;
        key.FromString((*it));
        termSigns.push_back(key);
        ++it;
    }
}

string HighFreqVocabularyCreator::GetAdaptiveHFDictName(const string& indexName,
                                                        const std::shared_ptr<DictionaryConfig>& dictConfig)
{
    string dictName = "";
    if (dictConfig) {
        dictName = indexName + "_" + dictConfig->GetDictName();
    } else {
        dictName = indexName;
    }
    return dictName;
}
}} // namespace indexlib::config
