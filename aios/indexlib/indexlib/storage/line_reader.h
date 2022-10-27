#ifndef __INDEXLIB_LINE_READER_H
#define __INDEXLIB_LINE_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/storage/file_wrapper.h"

IE_NAMESPACE_BEGIN(storage);

class LineReader
{
public:
    LineReader();
    ~LineReader();
public:
    // throw exception if open failed
    void Open(const std::string &fileName);
    void Open(const storage::FileWrapperPtr fileWrapper)
    {
        mFile = fileWrapper;
    }
    bool NextLine(std::string &line);
private:
    storage::FileWrapperPtr mFile;
    char *mBuf;
    size_t mSize;
    size_t mCursor;
private:
    static const uint32_t DEFAULT_BUFFER_SIZE = 4096;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LineReader);

IE_NAMESPACE_END(storage);

#endif //__INDEXLIB_LINE_READER_H
