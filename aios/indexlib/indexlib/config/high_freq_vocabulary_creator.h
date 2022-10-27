#ifndef __INDEXLIB_HIGH_FREQ_VOCABULARY_CREATOR_H
#define __INDEXLIB_HIGH_FREQ_VOCABULARY_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/high_frequency_vocabulary.h"
#include "indexlib/config/dictionary_config.h"
#include "indexlib/storage/archive_folder.h"
#include "indexlib/storage/archive_file.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(config);

class HighFreqVocabularyCreator
{
public:
    HighFreqVocabularyCreator();
    ~HighFreqVocabularyCreator();

public:
    static HighFrequencyVocabularyPtr CreateVocabulary(
            const std::string& indexName, IndexType indexType,
            const DictionaryConfigPtr& dictConfig);

    static HighFrequencyVocabularyPtr LoadAdaptiveVocabulary(
            const storage::ArchiveFolderPtr& adaptiveHighFrequeryFolder,
            const std::string& indexName, IndexType indexType,
            const DictionaryConfigPtr& dictConfig);

    static HighFrequencyVocabularyPtr LoadAdaptiveVocabulary(
            const file_system::DirectoryPtr& adaptiveDictDir,
            const std::string& indexName, IndexType indexType,
            const DictionaryConfigPtr& dictConfig);

    //for dump
    static HighFrequencyVocabularyPtr CreateAdaptiveVocabulary(
            const std::string& indexName, IndexType indexType,
            const DictionaryConfigPtr& dictConfig);

private:
    static std::string GetAdaptiveHFDictName(const std::string& indexName, 
            const DictionaryConfigPtr& dictConfig);
    static bool GetAdaptiveHFTermSigns(const storage::FileWrapperPtr& archiveFile,
            std::vector<uint64_t>& termSigns);
    static void GetAdaptiveHFTermSigns(const std::string& fileContent,
            const std::string& adaptiveFilePath, std::vector<uint64_t>& termSigns);
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(HighFreqVocabularyCreator);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_HIGH_FREQ_VOCABULARY_CREATOR_H
