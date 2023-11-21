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
#include <unordered_map>

#include "autil/Log.h"
#include "indexlib/config/truncate_index_config.h"
#include "indexlib/file_system/archive/ArchiveFolder.h"
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib::file_system {
class Directory;
typedef std::shared_ptr<Directory> DirectoryPtr;
} // namespace indexlib::file_system

namespace indexlib { namespace config {

class TruncateTermVocabulary
{
public:
    typedef std::unordered_map<dictkey_t, int32_t> TermFreqMap;

public:
    TruncateTermVocabulary(const file_system::ArchiveFolderPtr& metaFolder);
    ~TruncateTermVocabulary();

public:
    void Init(const std::vector<std::string>& truncIndexNames);
    void Init(const file_system::DirectoryPtr& truncMetaDir, const std::vector<std::string>& truncIndexNames);
    size_t GetTermCount() const;

    bool Lookup(const index::DictKeyInfo& key) const;
    bool LookupTF(const index::DictKeyInfo& key, int32_t& tf) const;

private:
    void LoadTruncateTerms(const file_system::FileReaderPtr& fileReader);
    void LoadTruncateTerms(const file_system::DirectoryPtr& truncMetaDir, const std::string& truncIndexName);
    index::DictKeyInfo ExtractTermKey(const std::string& truncMetaLine);

    void LoadSingleTermKey(const std::string& str);

private:
    file_system::ArchiveFolderPtr mMetaFolder;
    TermFreqMap mTerms;
    bool mHasNullTerm;
    int32_t mNullTermTf;

private:
    friend class TruncateTermVocabularyTest;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<TruncateTermVocabulary> TruncateTermVocabularyPtr;
}} // namespace indexlib::config
