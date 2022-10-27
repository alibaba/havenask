#include "indexlib/config/high_frequency_vocabulary.h"
#include <autil/HashAlgorithm.h>
#include <autil/StringTokenizer.h>
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/storage/file_wrapper.h"
#include "indexlib/storage/archive_file.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, HighFrequencyVocabulary);

HighFrequencyVocabulary::HighFrequencyVocabulary() 
{
}

HighFrequencyVocabulary::~HighFrequencyVocabulary() 
{
}

void HighFrequencyVocabulary::Init(const std::string& vocabularyName, 
                                   IndexType indexType, const std::string& content)
{
    mVocabularyName = vocabularyName;
    InitWithContent(content, indexType);
}

void HighFrequencyVocabulary::InitWithContent(
        const std::string& content, IndexType indexType)
{
    mHasher.reset(util::KeyHasherFactory::Create(indexType));
    autil::StringTokenizer st(content, ";",
                              StringTokenizer::TOKEN_IGNORE_EMPTY
                              | StringTokenizer::TOKEN_TRIM);
    autil::StringTokenizer::Iterator it = st.begin();
    while(it != st.end())
    {
        dictkey_t key;
        if (mHasher->GetHashKey((*it).c_str(), key))
        {
            AddKey(key);
        }
        else
        {
            IE_LOG(DEBUG, "dict value [%s] can not convert to vocabulary key",
                   (*it).c_str());
        }
        ++it;
    }
}
 
bool HighFrequencyVocabulary::Lookup(const util::Term& term)
{
    dictkey_t key;
    if (mHasher->GetHashKey(term, key))
    {
        return Lookup(key);
    }
    return false;
}

bool HighFrequencyVocabulary::Lookup(const std::string& term)
{
    dictkey_t key;
    if (mHasher->GetHashKey(term.c_str(), key))
    {
        return Lookup(key);
    }
    return false;
}

bool HighFrequencyVocabulary::Lookup(uint64_t termSign)
{
    return mTerms.find(termSign) != mTerms.end();    
}

void HighFrequencyVocabulary::Clear()
{
    mTerms.clear();
}

void HighFrequencyVocabulary::AddKey(uint64_t termSign)
{
    mTerms.insert(termSign);
}

void HighFrequencyVocabulary::DumpTerms(const ArchiveFolderPtr& archiveFolder)
{
    FileWrapperPtr dataFile = archiveFolder->GetInnerFile(mVocabularyName, fslib::WRITE);
    for (TermSet::const_iterator iter = mTerms.begin();
         iter != mTerms.end(); iter++)
    {
        uint64_t sign = *iter;
        stringstream ss;
        ss << sign << endl;
        string line = ss.str();
        dataFile->Write(line.c_str(), line.size());
    }
    dataFile->Close();
}

IE_NAMESPACE_END(config);

