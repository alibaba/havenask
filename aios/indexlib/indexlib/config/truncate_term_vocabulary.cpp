#include <fslib/fslib.h>
#include <autil/StringUtil.h>
#include "indexlib/config/truncate_term_vocabulary.h"
#include "indexlib/storage/line_reader.h"
#include "indexlib/misc/exception.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/directory.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace autil;

IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, TruncateTermVocabulary);

TruncateTermVocabulary::TruncateTermVocabulary(
        const ArchiveFolderPtr& metaFolder)
    : mMetaFolder(metaFolder)
{
}

TruncateTermVocabulary::~TruncateTermVocabulary() 
{
}

void TruncateTermVocabulary::Init(
        const vector<string>& truncIndexNames)
{
    for (size_t i = 0; i < truncIndexNames.size(); i++)
    {
        FileWrapperPtr file = mMetaFolder->GetInnerFile(truncIndexNames[i], fslib::READ);
        if (file)
        {
            LoadTruncateTerms(file);
        }
    }
}

void TruncateTermVocabulary::Init(const file_system::DirectoryPtr& truncMetaDir,
                                  const vector<string>& truncIndexNames)
{
    for (size_t i = 0; i < truncIndexNames.size(); i++)
    {
        LoadTruncateTerms(truncMetaDir, truncIndexNames[i]);
    }
}

bool TruncateTermVocabulary::Lookup(dictkey_t key) const
{
    return mTerms.find(key) != mTerms.end();
}

bool TruncateTermVocabulary::LookupTF(dictkey_t key, int32_t& tf) const
{
    TermFreqMap::const_iterator it = mTerms.find(key);
    if (it == mTerms.end())
    {
        return false;
    }
    tf = it->second;
    return true;
}

size_t TruncateTermVocabulary::GetTermCount() const
{
    return mTerms.size();
}

void TruncateTermVocabulary::LoadTruncateTerms(
        const FileWrapperPtr& fileWrapper)
{
    LineReader reader;
    reader.Open(fileWrapper);
    dictkey_t key;
    string line;
    while (reader.NextLine(line))
    {
        key = ExtractTermKey(line);
        int32_t termFreq = 0;
        LookupTF(key, termFreq);
        ++termFreq;
        mTerms[key] = termFreq;
        line.clear();
    }
}

dictkey_t TruncateTermVocabulary::ExtractTermKey(
        const string& truncMetaLine)
{
    vector<string> strVec = StringUtil::split(truncMetaLine, "\t");
    assert(strVec.size() == 2);
    
    dictkey_t key;
    if (!StringUtil::fromString(strVec[0], key))
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "invalid key: %s", 
                             strVec[0].c_str());
    }
    return key;
}

void TruncateTermVocabulary::LoadTruncateTerms(const file_system::DirectoryPtr& truncMetaDir,
        const string& truncIndexName)
{
    string fileContent;
    if (!truncMetaDir->LoadMayNonExist(truncIndexName, fileContent, true))
    {
        return;
    }
    StringTokenizer st(fileContent, "\n",
                       StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    for (auto it = st.begin(); it != st.end(); ++it)
    {
        dictkey_t key = ExtractTermKey(*it);
        int32_t termFreq = 0;
        LookupTF(key, termFreq);
        ++termFreq;
        mTerms[key] = termFreq;
    }
}

IE_NAMESPACE_END(config);

