#include "indexlib/file_system/buffered_file_output_stream.h"
#include "indexlib/file_system/buffered_file_node.h"
#include "indexlib/storage/file_buffer.h"
#include "indexlib/util/path_util.h"
#include <fslib/fs/FileSystem.h>

using namespace std;

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, BufferedFileOutputStream);

void BufferedFileOutputStream::Open(const std::string& path) {
    mPath = path;
    size_t bufferSize = mUseRaid ? mRaidConfig->bufferSizeThreshold : 0;
    mBuffer = std::make_unique<storage::FileBuffer>(bufferSize);
}

size_t BufferedFileOutputStream::Write(const void* buffer, size_t length)
{
    if (mBuffer && length <= mBuffer->GetFreeSpace())
    {
        mBuffer->CopyToBuffer((char*)buffer, length);
        return length;
    }
    InitFileNode(false);
    auto ret = FlushBuffer();
    return mFileNode->Write(buffer, length) + ret;
}

void BufferedFileOutputStream::Close()
{
    InitFileNode(true);
    FlushBuffer();
    mFileNode->Close();
}

size_t BufferedFileOutputStream::GetLength() const {
    return mBuffer ? mBuffer->GetCursor() : mFileNode->GetLength();
}

void BufferedFileOutputStream::InitFileNode(bool onClose)  {
    if (mFileNode)
    {
        return;
    }
    string path = mPath;
    if (mUseRaid && !onClose)
    {
        path = mRaidConfig->MakeRaidPath(path);
    }
    mFileNode.reset(CreateFileNode());
    mFileNode->Open(path, FSOT_BUFFERED);
}
BufferedFileNode* BufferedFileOutputStream::CreateFileNode() const
{
    return new BufferedFileNode(false);
}

size_t BufferedFileOutputStream::FlushBuffer() {
    if (!mBuffer)
    {
        return 0;
    }
    auto ret = mFileNode->Write(mBuffer->GetBaseAddr(), mBuffer->GetCursor());
    mBuffer.reset();
    return ret;
}

IE_NAMESPACE_END(file_system);

