#ifndef __INDEXLIB_SWAP_MMAP_FILE_READER_H
#define __INDEXLIB_SWAP_MMAP_FILE_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/file_system/swap_mmap_file_node.h"

IE_NAMESPACE_BEGIN(file_system);

class SwapMmapFileReader : public FileReader
{
public:
    SwapMmapFileReader(const SwapMmapFileNodePtr& fileNode);
    ~SwapMmapFileReader();
    
public:
    void Open() override
    {
        assert(mFileNode);
    }
    
    size_t Read(void* buffer, size_t length, size_t offset, ReadOption option = ReadOption()) override
    {
        return mFileNode ? mFileNode->Read(buffer, length, offset, option) : 0;
    }
    
    size_t Read(void* buffer, size_t length, ReadOption option = ReadOption()) override
    {
        assert(false);
        return 0;
    }
    
    util::ByteSliceList* Read(size_t length, size_t offset, ReadOption option = ReadOption()) override
    {
        return mFileNode ? mFileNode->Read(length, offset, option) : NULL;
    }
    
    void* GetBaseAddress() const override
    {
        return mFileNode ? mFileNode->GetBaseAddress() : NULL;
    }
    
    size_t GetLength() const override
    {
        return mFileNode ? mFileNode->GetLength() : 0;
    }
    
    const std::string& GetPath() const override
    {
        if (!mFileNode)
        {
            static std::string tmp;
            return tmp;
        }
        return mFileNode->GetPath();
    }
    
    void Close() override
    {
        mFileNode.reset();
    }
    
    FSOpenType GetOpenType() override
    {
        return mFileNode ? mFileNode->GetOpenType() : FSOT_UNKNOWN;
    }

private:
    FileNodePtr GetFileNode() const override
    { return mFileNode; }

private:
    SwapMmapFileNodePtr mFileNode;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SwapMmapFileReader);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_SWAP_MMAP_FILE_READER_H
