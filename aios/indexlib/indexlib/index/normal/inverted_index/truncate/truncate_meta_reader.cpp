#include <fslib/fslib.h>
#include <autil/StringUtil.h>
#include "indexlib/index/normal/inverted_index/truncate/truncate_meta_reader.h"
#include "indexlib/misc/exception.h"
#include "indexlib/storage/line_reader.h"

using namespace std;
using namespace fslib;
using namespace autil;

IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, TruncateMetaReader);

TruncateMetaReader::TruncateMetaReader(bool desc) 
    : mDesc(desc)
{
}

TruncateMetaReader::~TruncateMetaReader() 
{
}

void TruncateMetaReader::Open(const storage::FileWrapperPtr& file)
{
    LineReader reader;
    reader.Open(file);
    string line;
    while (reader.NextLine(line))
    {
        AddOneRecord(line);
    }
}

void TruncateMetaReader::Open(const string &filePath)
{
    LineReader reader;
    reader.Open(filePath);
    string line;
    while (reader.NextLine(line))
    {
        AddOneRecord(line);
    }
}

void TruncateMetaReader::AddOneRecord(const string &line)
{
    vector<string> strVec = StringUtil::split(line, "\t");
    assert(strVec.size() == 2);
    
    dictkey_t key;
    int64_t value;

    if (!StringUtil::fromString(strVec[0], key))
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "invalid key: %s", 
                             strVec[0].c_str());
    }
    if (!StringUtil::fromString(strVec[1], value))
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "invalid value: %s", 
                             strVec[1].c_str());
    }

    if (mDesc)
    {
        int64_t max = numeric_limits<int64_t>::max();
        mDict[key] = pair<dictkey_t, int64_t>(value, max);
    }
    else
    {
        int64_t min = numeric_limits<int64_t>::min();
        mDict[key] = pair<dictkey_t, int64_t>(min, value);
    }
}

bool TruncateMetaReader::IsExist(dictkey_t key)
{
    return mDict.find(key) != mDict.end();
}

bool TruncateMetaReader::Lookup(dictkey_t key, int64_t &min, int64_t &max)
{
    DictType::iterator it = mDict.find(key);
    if (it == mDict.end())
    {
        return false;
    }
    min = it->second.first;
    max = it->second.second;
    return true;
}

IE_NAMESPACE_END(index);

