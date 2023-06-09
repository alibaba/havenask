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
#include "indexlib/index/inverted_index/config/TruncateTermVocabulary.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/archive/LineReader.h"
// #include "indexlib/file_system/fslib/FslibWrapper.h"

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.config, TruncateTermVocabulary);

TruncateTermVocabulary::TruncateTermVocabulary(
    const std::shared_ptr<indexlib::file_system::ArchiveFolder>& legacyArchiveFolder)
    : _legacyArchiveFolder(legacyArchiveFolder)
    , _hasNullTerm(false)
    , _nullTermTf(0)
{
}
Status TruncateTermVocabulary::Init(const std::vector<std::string>& truncIndexNames)
{
    for (size_t i = 0; i < truncIndexNames.size(); i++) {
        auto file = _legacyArchiveFolder->CreateFileReader(truncIndexNames[i]).GetOrThrow();
        if (file) {
            auto st = LoadTruncateTerms(file);
            if (!st.IsOK()) {
                AUTIL_LOG(ERROR, "load truncate term failed.");
                return st;
            }
        }
    }
    return Status::OK();
}

Status TruncateTermVocabulary::Init(const std::shared_ptr<indexlib::file_system::IDirectory>& truncMetaDir,
                                    const std::vector<std::string>& truncIndexNames)
{
    for (size_t i = 0; i < truncIndexNames.size(); i++) {
        RETURN_IF_STATUS_ERROR(LoadTruncateTerms(truncMetaDir, truncIndexNames[i]), "load truncate term failed %s",
                               truncIndexNames[i].c_str());
    }
    return Status::OK();
}

bool TruncateTermVocabulary::Lookup(const indexlib::index::DictKeyInfo& key) const
{
    return key.IsNull() ? _hasNullTerm : _terms.find(key.GetKey()) != _terms.end();
}

bool TruncateTermVocabulary::LookupTF(const indexlib::index::DictKeyInfo& key, int32_t& tf) const
{
    if (key.IsNull()) {
        tf = _nullTermTf;
        return _hasNullTerm;
    }

    TermFreqMap::const_iterator it = _terms.find(key.GetKey());
    if (it == _terms.end()) {
        return false;
    }
    tf = it->second;
    return true;
}

size_t TruncateTermVocabulary::GetTermCount() const { return _hasNullTerm ? _terms.size() + 1 : _terms.size(); }

indexlib::index::DictKeyInfo TruncateTermVocabulary::ExtractTermKey(const std::string& truncMetaLine)
{
    std::vector<std::string> strVec = autil::StringUtil::split(truncMetaLine, "\t");
    assert(strVec.size() == 2);
    indexlib::index::DictKeyInfo key;
    key.FromString(strVec[0]);
    return key;
}

Status TruncateTermVocabulary::LoadTruncateTerms(const std::shared_ptr<indexlib::file_system::IDirectory>& truncMetaDir,
                                                 const std::string& truncIndexName)
{
    std::string fileContent;
    indexlib::file_system::ReaderOption option =
        indexlib::file_system::ReaderOption::PutIntoCache(indexlib::file_system::FSOT_MEM);
    option.mayNonExist = true;
    auto ec = truncMetaDir->Load(truncIndexName, option, fileContent).Code();
    if (ec != indexlib::file_system::FSEC_OK) {
        AUTIL_LOG(WARN, "load truncate term failed, file[%s]", truncIndexName.c_str());
        return indexlib::file_system::toStatus(ec);
    }
    autil::StringTokenizer st(fileContent, "\n",
                              autil::StringTokenizer::TOKEN_IGNORE_EMPTY | autil::StringTokenizer::TOKEN_TRIM);
    for (auto it = st.begin(); it != st.end(); ++it) {
        LoadSingleTermKey(*it);
    }
    return Status::OK();
}

Status TruncateTermVocabulary::LoadTruncateTerms(const std::shared_ptr<indexlib::file_system::FileReader>& fileReader)
{
    indexlib::file_system::LineReader reader;
    reader.Open(fileReader);
    std::string line;
    indexlib::file_system::ErrorCode ec = indexlib::file_system::FSEC_OK;
    while (reader.NextLine(line, ec)) {
        LoadSingleTermKey(line);
        line.clear();
    }
    if (ec != indexlib::file_system::FSEC_OK) {
        AUTIL_LOG(ERROR, "next line failed, file[%s]", fileReader->DebugString().c_str());
        return Status::InternalError("next line failed");
    }
    return Status::OK();
}

void TruncateTermVocabulary::LoadSingleTermKey(const std::string& str)
{
    indexlib::index::DictKeyInfo key = ExtractTermKey(str);
    int32_t termFreq = 0;
    LookupTF(key, termFreq);
    ++termFreq;
    if (key.IsNull()) {
        _hasNullTerm = true;
        _nullTermTf = termFreq;
    } else {
        _terms[key.GetKey()] = termFreq;
    }
}
} // namespace indexlibv2::config
