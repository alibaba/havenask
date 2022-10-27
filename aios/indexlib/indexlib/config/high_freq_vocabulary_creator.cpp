#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>
#include "indexlib/config/high_freq_vocabulary_creator.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/directory.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, HighFreqVocabularyCreator);

HighFreqVocabularyCreator::HighFreqVocabularyCreator() 
{
}

HighFreqVocabularyCreator::~HighFreqVocabularyCreator() 
{
}

HighFrequencyVocabularyPtr HighFreqVocabularyCreator::CreateVocabulary(
        const std::string& indexName, IndexType indexType,
        const DictionaryConfigPtr& dictConfig)
{
    if (!dictConfig)
    {
        return HighFrequencyVocabularyPtr();
    }
    HighFrequencyVocabularyPtr volcabulary(new HighFrequencyVocabulary());
    volcabulary->Init(indexName, indexType, dictConfig->GetContent());
    return volcabulary;
}

//check index need create adaptive vocabulary
HighFrequencyVocabularyPtr HighFreqVocabularyCreator::LoadAdaptiveVocabulary(
        const ArchiveFolderPtr& adaptiveHighFrequeryFolder,
        const std::string& indexName, IndexType indexType,
        const DictionaryConfigPtr& dictConfig)
{
    HighFrequencyVocabularyPtr volcabulary;
    if (!adaptiveHighFrequeryFolder)
    {
        return volcabulary;
    }
    volcabulary.reset(new HighFrequencyVocabulary());
    string volcabularyName = GetAdaptiveHFDictName(indexName, dictConfig);
    string dictContent = "";
    if (dictConfig)
    {
        dictContent = dictConfig->GetContent();
    }
    volcabulary->Init(volcabularyName, indexType, dictContent);

    FileWrapperPtr adaptiveHighFrequeryFile = 
        adaptiveHighFrequeryFolder->GetInnerFile(volcabularyName, fslib::READ);
    vector<uint64_t> termSigns;
    if (!GetAdaptiveHFTermSigns(adaptiveHighFrequeryFile, termSigns))
    {
        IE_LOG(WARN, "get adaptive hf_frequency bitmap terms from [%s] failed!",
               volcabularyName.c_str());
        return HighFrequencyVocabularyPtr();
    }
    for (size_t i = 0; i < termSigns.size(); ++i)
    {
        volcabulary->AddKey(termSigns[i]);
    }
    adaptiveHighFrequeryFile->Close();
    return volcabulary;
}

HighFrequencyVocabularyPtr HighFreqVocabularyCreator::LoadAdaptiveVocabulary(
        const file_system::DirectoryPtr& adaptiveDictDir,        
        const std::string& indexName, IndexType indexType,
        const DictionaryConfigPtr& dictConfig)
{
    HighFrequencyVocabularyPtr volcabulary;

    volcabulary.reset(new HighFrequencyVocabulary());
    string volcabularyName = GetAdaptiveHFDictName(indexName, dictConfig);
    string dictContent = "";
    if (dictConfig)
    {
        dictContent = dictConfig->GetContent();
    }
    volcabulary->Init(volcabularyName, indexType, dictContent);

    string adaptiveHighFrequeryFile = storage::FileSystemWrapper::JoinPath(
            adaptiveDictDir->GetPath(), volcabularyName);
    string fileContent;
    if (!adaptiveDictDir->LoadMayNonExist(volcabularyName, fileContent, true))
    {
        IE_LOG(WARN, "get adaptive hf_frequency bitmap terms from [%s] failed!", 
               adaptiveHighFrequeryFile.c_str());
        return HighFrequencyVocabularyPtr();
    }
    vector<uint64_t> termSigns;
    GetAdaptiveHFTermSigns(fileContent, adaptiveHighFrequeryFile, termSigns);
    for (size_t i = 0; i < termSigns.size(); ++i)
    {
        volcabulary->AddKey(termSigns[i]);
    }
    return volcabulary;
}

HighFrequencyVocabularyPtr HighFreqVocabularyCreator::CreateAdaptiveVocabulary(
        const std::string& indexName, IndexType indexType,
        const DictionaryConfigPtr& dictConfig)
{
    HighFrequencyVocabularyPtr volcabulary(new HighFrequencyVocabulary());
    string volcabularyName = GetAdaptiveHFDictName(indexName, dictConfig);
    string dictContent = "";
    if (dictConfig)
    {
        dictContent = dictConfig->GetContent();
    }
    volcabulary->Init(volcabularyName, indexType, dictContent);
    return volcabulary;
}

bool HighFreqVocabularyCreator::GetAdaptiveHFTermSigns(
        const FileWrapperPtr& archiveFile,
        vector<uint64_t>& termSigns)
{
    if (!archiveFile)
    {
        return false;
    }

    string fileContent;
    try
    {
        if (!FileSystemWrapper::AtomicLoad(archiveFile.get(), fileContent))
        {
            return false;
        }
    }
    catch (...)
    {
        throw;
    }
    
    StringTokenizer st(fileContent, "\n",
                       StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);

    StringTokenizer::Iterator it = st.begin();
    while(it != st.end())
    {
        uint64_t sign = 0;
        if (!StringUtil::strToUInt64((*it).c_str(), sign))
        {
            INDEXLIB_FATAL_ERROR(IndexCollapsed,
                                 "invalid high frequency signiture [%s] in file [%s]",
                                 (*it).c_str(), archiveFile->GetFileName());
        }
        termSigns.push_back(sign);
        ++it;
    }
    return true;
}

void HighFreqVocabularyCreator::GetAdaptiveHFTermSigns(
        const string& fileContent, const string& adaptiveFilePath, vector<uint64_t>& termSigns)
{
    StringTokenizer st(fileContent, "\n",
                       StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);

    StringTokenizer::Iterator it = st.begin();
    while(it != st.end())
    {
        uint64_t sign = 0;
        if (!StringUtil::strToUInt64((*it).c_str(), sign))
        {
            INDEXLIB_FATAL_ERROR(IndexCollapsed,
                    "invalid high frequency signiture [%s] in file [%s]",
                    (*it).c_str(), adaptiveFilePath.c_str());
        }
        termSigns.push_back(sign);
        ++it;
    }
}

std::string HighFreqVocabularyCreator::GetAdaptiveHFDictName(
        const string& indexName, const DictionaryConfigPtr& dictConfig)
{
    string dictName = "";
    if (dictConfig)
    {
        dictName = indexName + "_" + dictConfig->GetDictName();
    }
    else
    {
        dictName = indexName;
    }
    return dictName;
}

IE_NAMESPACE_END(config);

