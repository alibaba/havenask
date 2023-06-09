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
#include <unordered_set>

#include "autil/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/index/common/DictHasher.h"
#include "indexlib/index/common/Term.h"

namespace indexlib::file_system {
class ArchiveFolder;
class RelocatableFolder;
class IDirectory;
class FileWriter;
} // namespace indexlib::file_system

namespace indexlib::index {
class DictKeyInfo;
}

namespace indexlib::config {

class HighFrequencyVocabulary
{
public:
    typedef std::unordered_set<dictkey_t> TermSet;

public:
    HighFrequencyVocabulary();
    explicit HighFrequencyVocabulary(const std::string& nullTermLiteralStr);
    HighFrequencyVocabulary(const HighFrequencyVocabulary& other) = default;
    ~HighFrequencyVocabulary();

public:
    void Init(const std::string& vocabularyName, InvertedIndexType indexType, const std::string& content,
              const util::KeyValueMap& dictHashParams);
    void AddKey(const index::DictKeyInfo& termKey);
    bool Lookup(const index::DictKeyInfo& termKey);

    void Clear();
    Status DumpTerms(const std::shared_ptr<file_system::ArchiveFolder>& archiveFolder);
    Status DumpTerms(const std::shared_ptr<indexlib::file_system::RelocatableFolder>& dir);

    // for test
    size_t GetTermCount() const { return _terms.size(); }
    bool Lookup(const std::string& term);
    bool Lookup(const index::Term& term);
    const std::string& GetVocabularyName() const { return _vocabularyName; }

private:
    void InitWithContent(const std::string& content, InvertedIndexType indexType,
                         const util::KeyValueMap& dictHashParams);
    Status DumpTerms(const std::shared_ptr<file_system::FileWriter>& writer);

protected:
    index::IndexDictHasher _dictHasher;
    std::string _nullTermLiteralStr;
    std::string _vocabularyName;
    TermSet _terms;
    bool _hasNullTerm = false;

private:
    friend class HighFrequencyVocabularyTest;
    friend class HighFreqVocabularyCreatorTest;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::config
