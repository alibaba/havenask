#ifndef __INDEXLIB_TRUNCATE_META_READER_H
#define __INDEXLIB_TRUNCATE_META_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/storage/file_wrapper.h"
#include "indexlib/common_define.h"
#include <stdlib.h>

IE_NAMESPACE_BEGIN(index);

class TruncateMetaReader
{
public:
    typedef std::pair<int64_t, int64_t> Range;
    typedef std::map<key_t, Range> DictType;

public:
    TruncateMetaReader(bool desc);
    virtual ~TruncateMetaReader();

public:
    void Open(const storage::FileWrapperPtr& archiveFile);
    void Open(const std::string &filePath);
    bool IsExist(dictkey_t key);

    size_t Size() { return mDict.size(); }
    virtual bool Lookup(dictkey_t key, int64_t &min, int64_t &max);

private:
    void AddOneRecord(const std::string &line);

public:
    // for test
    void SetDict(const DictType &dict)
    {
        mDict = dict;
    }
    
protected:
    bool mDesc;
    DictType mDict;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateMetaReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TRUNCATE_META_READER_H
