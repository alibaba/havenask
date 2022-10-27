#ifndef __INDEXLIB_BUFFERED_FILE_OUTPUT_STREAM_H
#define __INDEXLIB_BUFFERED_FILE_OUTPUT_STREAM_H

#include <memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/storage/raid_config.h"
#include "indexlib/file_system/buffered_file_node.h"
#include "indexlib/storage/file_buffer.h"
#include <string>

DECLARE_REFERENCE_CLASS(file_system, BufferedFileNode);

IE_NAMESPACE_BEGIN(file_system);

class BufferedFileOutputStream
{
public:
    BufferedFileOutputStream(const storage::RaidConfigPtr& raidConfig)
        : mRaidConfig(raidConfig)
        , mUseRaid(false)
    {
        mUseRaid = mRaidConfig && mRaidConfig->useRaid;
    }
    virtual ~BufferedFileOutputStream() {}

public:
    void Open(const std::string&path);
    size_t Write(const void* buffer, size_t length);
    virtual void Close();

    size_t GetLength() const;

private:
    void InitFileNode(bool onClose) ;
    size_t FlushBuffer();

protected:
    // for mock
    virtual BufferedFileNode* CreateFileNode() const;

private:
    storage::RaidConfigPtr mRaidConfig;
    bool mUseRaid;
    std::string mPath;
    std::unique_ptr<BufferedFileNode> mFileNode;
    std::unique_ptr<storage::FileBuffer> mBuffer;

private:
    IE_LOG_DECLARE();
};

using BufferedFileOutputStreamPtr = std::shared_ptr<BufferedFileOutputStream>;

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_BUFFERED_FILE_OUTPUT_STREAM_H
