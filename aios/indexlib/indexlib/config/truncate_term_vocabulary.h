#ifndef __INDEXLIB_TRUNCATE_TERM_VOCABULARY_H
#define __INDEXLIB_TRUNCATE_TERM_VOCABULARY_H

#include <tr1/memory>
#include <tr1/unordered_map>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/truncate_index_config.h"
#include "indexlib/storage/archive_folder.h"
#include "indexlib/storage/file_wrapper.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(config);

class TruncateTermVocabulary
{
public:
    typedef std::tr1::unordered_map<dictkey_t, int32_t> TermFreqMap;

public:
    TruncateTermVocabulary(const storage::ArchiveFolderPtr& metaFolder);
    ~TruncateTermVocabulary();

public:
    void Init(const std::vector<std::string>& truncIndexNames);
    void Init(const file_system::DirectoryPtr& truncMetaDir,
              const std::vector<std::string>& truncIndexNames);
    size_t GetTermCount() const;
    bool Lookup(dictkey_t key) const;
    bool LookupTF(dictkey_t key, int32_t& tf) const;

private:
    void LoadTruncateTerms(const storage::FileWrapperPtr& fileWrapper);
    void LoadTruncateTerms(const file_system::DirectoryPtr& truncMetaDir,
                           const std::string& truncIndexName);
    dictkey_t ExtractTermKey(const std::string& truncMetaLine);

private:
    storage::ArchiveFolderPtr mMetaFolder;
    TermFreqMap mTerms;

private:
    friend class TruncateTermVocabularyTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateTermVocabulary);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_TRUNCATE_TERM_VOCABULARY_H
