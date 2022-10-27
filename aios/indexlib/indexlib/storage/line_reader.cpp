#include "indexlib/storage/line_reader.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(storage);
IE_LOG_SETUP(storage, LineReader);

LineReader::LineReader() 
    : mBuf(NULL)
    , mSize(0)
    , mCursor(0)
{
    mBuf = new char[DEFAULT_BUFFER_SIZE];
}

LineReader::~LineReader() 
{
    if (mBuf)
    {
        delete [] mBuf;
        mBuf = NULL;
    }

    if (mFile)
    {
        try
        {
            mFile->Close();
            mFile.reset();
        }
        catch(...)
        {}
    }
}

void LineReader::Open(const string &fileName)
{
    mFile.reset(FileSystemWrapper::OpenFile(fileName, fslib::READ));
}

bool LineReader::NextLine(string &line)
{
    if (!mFile)
    {
        return false;
    }
    line.clear();
    while (true)
    {
        if (mCursor == mSize)
        {
            mCursor = 0;
            mSize = mFile->Read(mBuf, DEFAULT_BUFFER_SIZE);
            if (mSize == 0)
            {
                //assert(mFile->IsEof());
                return !line.empty();
            }
        }
        size_t pos = mCursor;
        for (; pos < mSize && mBuf[pos] != '\n'; pos++);
        if (pos < mSize)
        {
            line.append(mBuf + mCursor, pos - mCursor);
            mCursor = pos + 1;
            break;
        }
        else 
        {
            line.append(mBuf + mCursor, pos - mCursor);
            mCursor = pos;
        }
    }
    return true;
}

IE_NAMESPACE_END(storage);

