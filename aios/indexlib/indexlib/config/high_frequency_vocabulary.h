#ifndef __INDEXLIB_HIGH_FREQUENCY_VOCABULARY_H
#define __INDEXLIB_HIGH_FREQUENCY_VOCABULARY_H

#include <tr1/memory>
#include <tr1/unordered_set>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/key_hasher_factory.h"
#include "indexlib/storage/archive_folder.h"

IE_NAMESPACE_BEGIN(config);

class HighFrequencyVocabulary 
{
public:
    typedef std::tr1::unordered_set<uint64_t> TermSet;

public:
    HighFrequencyVocabulary();

public:
    ~HighFrequencyVocabulary();

public:
    void Init(const std::string& vocabularyName, 
              IndexType indexType, const std::string& content);

    void AddKey(uint64_t termSign);
    bool Lookup(uint64_t termSign);
    void Clear();
    void DumpTerms(const storage::ArchiveFolderPtr& archiveFolder);

    // for test
    size_t GetTermCount() const { return mTerms.size(); }
    bool Lookup(const std::string& term);
    bool Lookup(const util::Term& term);
    const std::string& GetVocabularyName() const { return mVocabularyName; }
private:
    void InitWithContent(const std::string& content, IndexType indexType);

protected:
    std::string mVocabularyName;
    util::KeyHasherPtr mHasher;
    TermSet mTerms;

private:
    friend class HighFrequencyVocabularyTest;
    friend class HighFreqVocabularyCreatorTest;
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<HighFrequencyVocabulary> HighFrequencyVocabularyPtr;

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_HIGH_FREQUENCY_VOCABULARY_H
