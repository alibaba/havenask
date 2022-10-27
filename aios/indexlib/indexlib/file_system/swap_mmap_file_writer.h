#ifndef __INDEXLIB_SWAP_MMAP_FILE_WRITER_H
#define __INDEXLIB_SWAP_MMAP_FILE_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/file_system/swap_mmap_file_node.h"

DECLARE_REFERENCE_CLASS(file_system, Storage);

IE_NAMESPACE_BEGIN(file_system);

class SwapMmapFileWriter : public FileWriter
{
public:
    SwapMmapFileWriter(const SwapMmapFileNodePtr& fileNode,
                       Storage* storage);
    ~SwapMmapFileWriter();
    
public:
    void Open(const std::string& path) override;
    size_t Write(const void* buffer, size_t length) override;
    size_t GetLength() const override;
    const std::string& GetPath() const override;
    void Close() override;

    void SetDirty(bool dirty)
    {
        if (mFileNode)
        {
            mFileNode->SetDirty(dirty);
        }
    }

public:
    void Sync();
    void Resize(size_t fileLen);
    void WarmUp() { return mFileNode->WarmUp(); }
    void* GetBaseAddress() const { return mFileNode->GetBaseAddress(); }
    void Truncate(size_t fileLen) { Resize(fileLen); }
    
private:
    SwapMmapFileNodePtr mFileNode;
    Storage* mStorage;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SwapMmapFileWriter);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_SWAP_MMAP_FILE_WRITER_H
