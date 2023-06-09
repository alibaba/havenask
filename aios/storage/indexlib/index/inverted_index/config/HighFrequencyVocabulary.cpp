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
#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"

#include "autil/HashAlgorithm.h"
#include "autil/StringTokenizer.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/archive/ArchiveFile.h"
#include "indexlib/file_system/archive/ArchiveFolder.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/relocatable/RelocatableFolder.h"
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/common/KeyHasherWrapper.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;
using namespace indexlib::index;
using namespace indexlib::file_system;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.index, HighFrequencyVocabulary);

HighFrequencyVocabulary::HighFrequencyVocabulary() {}

HighFrequencyVocabulary::HighFrequencyVocabulary(const string& str) : _nullTermLiteralStr(str) {}

HighFrequencyVocabulary::~HighFrequencyVocabulary() {}

void HighFrequencyVocabulary::Init(const string& vocabularyName, InvertedIndexType indexType,
                                   const std::string& content, const util::KeyValueMap& dictHashParams)
{
    _vocabularyName = vocabularyName;
    InitWithContent(content, indexType, dictHashParams);
}

void HighFrequencyVocabulary::InitWithContent(const std::string& content, InvertedIndexType indexType,
                                              const KeyValueMap& dictHashParams)
{
    _dictHasher = IndexDictHasher(dictHashParams, indexType);
    autil::StringTokenizer st(content, ";", StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    autil::StringTokenizer::Iterator it = st.begin();
    while (it != st.end()) {
        if ((*it) == _nullTermLiteralStr) {
            AddKey(DictKeyInfo::NULL_TERM);
        }

        dictkey_t key;
        if (_dictHasher.CalcHashKey(*it, key)) {
            AddKey(DictKeyInfo(key));
        } else {
            AUTIL_LOG(DEBUG, "dict value [%s] can not convert to vocabulary key", (*it).c_str());
        }
        ++it;
    }
}

bool HighFrequencyVocabulary::Lookup(const index::Term& term)
{
    DictKeyInfo key;
    if (_dictHasher.GetHashKey(term, key)) {
        return Lookup(key);
    }
    return false;
}

bool HighFrequencyVocabulary::Lookup(const DictKeyInfo& termKey)
{
    if (termKey.IsNull()) {
        return _hasNullTerm;
    }
    return _terms.find(termKey.GetKey()) != _terms.end();
}

bool HighFrequencyVocabulary::Lookup(const std::string& term)
{
    if (term == _nullTermLiteralStr) {
        return Lookup(DictKeyInfo::NULL_TERM);
    }

    dictkey_t key;
    if (_dictHasher.CalcHashKey(term, key)) {
        return Lookup(DictKeyInfo(key));
    }
    return false;
}

void HighFrequencyVocabulary::Clear() { _terms.clear(); }

void HighFrequencyVocabulary::AddKey(const DictKeyInfo& termKey)
{
    if (termKey.IsNull()) {
        _hasNullTerm = true;
    } else {
        _terms.insert(termKey.GetKey());
    }
}

Status HighFrequencyVocabulary::DumpTerms(const ArchiveFolderPtr& archiveFolder)
{
    auto [status, dataFile] = archiveFolder->CreateFileWriter(_vocabularyName).StatusWith();
    RETURN_IF_STATUS_ERROR(status, "create file writer fail, vocabularyName[%s]", _vocabularyName.c_str());
    return DumpTerms(dataFile);
}

Status HighFrequencyVocabulary::DumpTerms(const std::shared_ptr<indexlib::file_system::RelocatableFolder>& dir)
{
    auto [status, dataFile] = dir->CreateFileWriter(_vocabularyName, indexlib::file_system::WriterOption::AtomicDump());
    RETURN_IF_STATUS_ERROR(status, "create file writer fail, vocabularyName[%s]", _vocabularyName.c_str());
    return DumpTerms(dataFile);
}

Status HighFrequencyVocabulary::DumpTerms(const std::shared_ptr<file_system::FileWriter>& dataFile)
{
    for (TermSet::const_iterator iter = _terms.begin(); iter != _terms.end(); iter++) {
        stringstream ss;
        ss << DictKeyInfo((*iter)).ToString() << endl;
        string line = ss.str();
        auto status = dataFile->Write(line.c_str(), line.size()).Status();
        RETURN_IF_STATUS_ERROR(status, "file write fail, [%s]", dataFile->DebugString().c_str());
    }

    if (_hasNullTerm) {
        stringstream ss;
        ss << DictKeyInfo::NULL_TERM.ToString() << endl;
        string line = ss.str();
        auto status = dataFile->Write(line.c_str(), line.size()).Status();
        RETURN_IF_STATUS_ERROR(status, "file write fail, [%s]", dataFile->DebugString().c_str());
    }
    auto status = dataFile->Close().Status();
    RETURN_IF_STATUS_ERROR(status, "file close fail, [%s]", dataFile->DebugString().c_str());
    return Status::OK();
}

}} // namespace indexlib::config
