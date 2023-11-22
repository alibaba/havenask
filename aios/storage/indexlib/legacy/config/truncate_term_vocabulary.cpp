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
#include "indexlib/config/truncate_term_vocabulary.h"

#include "autil/StringUtil.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/archive/LineReader.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;
using namespace indexlib::file_system;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, TruncateTermVocabulary);

TruncateTermVocabulary::TruncateTermVocabulary(const ArchiveFolderPtr& metaFolder)
    : mMetaFolder(metaFolder)
    , mHasNullTerm(false)
    , mNullTermTf(0)
{
}

TruncateTermVocabulary::~TruncateTermVocabulary() {}

void TruncateTermVocabulary::Init(const vector<string>& truncIndexNames)
{
    for (size_t i = 0; i < truncIndexNames.size(); i++) {
        auto file = mMetaFolder->CreateFileReader(truncIndexNames[i]).GetOrThrow();
        if (file) {
            LoadTruncateTerms(file);
        }
    }
}

void TruncateTermVocabulary::Init(const file_system::DirectoryPtr& truncMetaDir, const vector<string>& truncIndexNames)
{
    for (size_t i = 0; i < truncIndexNames.size(); i++) {
        LoadTruncateTerms(truncMetaDir, truncIndexNames[i]);
    }
}

bool TruncateTermVocabulary::Lookup(const index::DictKeyInfo& key) const
{
    return key.IsNull() ? mHasNullTerm : mTerms.find(key.GetKey()) != mTerms.end();
}

bool TruncateTermVocabulary::LookupTF(const index::DictKeyInfo& key, int32_t& tf) const
{
    if (key.IsNull()) {
        tf = mNullTermTf;
        return mHasNullTerm;
    }

    TermFreqMap::const_iterator it = mTerms.find(key.GetKey());
    if (it == mTerms.end()) {
        return false;
    }
    tf = it->second;
    return true;
}

size_t TruncateTermVocabulary::GetTermCount() const { return mHasNullTerm ? mTerms.size() + 1 : mTerms.size(); }

void TruncateTermVocabulary::LoadTruncateTerms(const FileReaderPtr& fileReader)
{
    LineReader reader;
    reader.Open(fileReader);
    string line;
    file_system::ErrorCode ec = FSEC_OK;
    while (reader.NextLine(line, ec)) {
        LoadSingleTermKey(line);
        line.clear();
    }
    THROW_IF_FS_ERROR(ec, "NextLine failed, file[%s]", fileReader->DebugString().c_str());
}

index::DictKeyInfo TruncateTermVocabulary::ExtractTermKey(const string& truncMetaLine)
{
    vector<string> strVec = StringUtil::split(truncMetaLine, "\t");
    assert(strVec.size() == 2);
    index::DictKeyInfo key;
    key.FromString(strVec[0]);
    return key;
}

void TruncateTermVocabulary::LoadTruncateTerms(const file_system::DirectoryPtr& truncMetaDir,
                                               const string& truncIndexName)
{
    string fileContent;
    if (!truncMetaDir->LoadMayNonExist(truncIndexName, fileContent, true)) {
        return;
    }
    StringTokenizer st(fileContent, "\n", StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    for (auto it = st.begin(); it != st.end(); ++it) {
        LoadSingleTermKey(*it);
    }
}

void TruncateTermVocabulary::LoadSingleTermKey(const string& str)
{
    index::DictKeyInfo key = ExtractTermKey(str);
    int32_t termFreq = 0;
    LookupTF(key, termFreq);
    ++termFreq;
    if (key.IsNull()) {
        mHasNullTerm = true;
        mNullTermTf = termFreq;
    } else {
        mTerms[key.GetKey()] = termFreq;
    }
}
}} // namespace indexlib::config
