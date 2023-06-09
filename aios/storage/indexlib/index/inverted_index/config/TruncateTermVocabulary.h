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

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/archive/ArchiveFolder.h"
#include "indexlib/index/common/DictKeyInfo.h"

namespace indexlibv2::config {

class TruncateTermVocabulary
{
public:
    using TermFreqMap = std::unordered_map<dictkey_t, int32_t>;

public:
    explicit TruncateTermVocabulary(const std::shared_ptr<indexlib::file_system::ArchiveFolder>& legacyArchiveFolder);
    ~TruncateTermVocabulary() = default;

public:
    Status Init(const std::vector<std::string>& truncIndexNames);
    Status Init(const std::shared_ptr<indexlib::file_system::IDirectory>& truncMetaDir,
                const std::vector<std::string>& truncIndexNames);
    size_t GetTermCount() const;

    bool Lookup(const indexlib::index::DictKeyInfo& key) const;
    bool LookupTF(const indexlib::index::DictKeyInfo& key, int32_t& tf) const;
    Status LoadTruncateTerms(const std::shared_ptr<indexlib::file_system::IDirectory>& truncMetaDir,
                             const std::string& truncIndexName);
    Status LoadTruncateTerms(const std::shared_ptr<indexlib::file_system::FileReader>& fileReader);

private:
    void LoadSingleTermKey(const std::string& str);
    indexlib::index::DictKeyInfo ExtractTermKey(const std::string& truncMetaLine);

private:
    std::shared_ptr<indexlib::file_system::ArchiveFolder> _legacyArchiveFolder;
    TermFreqMap _terms;
    bool _hasNullTerm;
    int32_t _nullTermTf;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
