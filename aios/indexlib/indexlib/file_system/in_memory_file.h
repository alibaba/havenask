#ifndef __INDEXLIB_IN_MEMORY_FILE_H
#define __INDEXLIB_IN_MEMORY_FILE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/file_system_define.h"
#include "indexlib/file_system/in_mem_file_node.h"

DECLARE_REFERENCE_CLASS(file_system, Storage);
DECLARE_REFERENCE_CLASS(file_system, InMemPackageFileWriter);

IE_NAMESPACE_BEGIN(file_system);

class InMemoryFile
{
public:
    // for not packaged file
    InMemoryFile(const std::string& filePath, uint64_t fileLen, Storage* storage,
                 const util::BlockMemoryQuotaControllerPtr& memController);

    ~InMemoryFile();

public:
    void* GetBaseAddress()
    {
        assert(mInMemFileNode);
        return mInMemFileNode->GetBaseAddress();
    }

    size_t GetLength() const
    {
        assert(mInMemFileNode);
        return mInMemFileNode->GetLength();
    }

    const std::string& GetPath() const
    {
        assert(mInMemFileNode);
        return mInMemFileNode->GetPath();
    }
    void Truncate(size_t newLength);
    void Sync(const FSDumpParam& param = FSDumpParam());

    // make file node in package file
    void SetInPackageFileWriter(InMemPackageFileWriter* packageFileWriter)
    { mPackageFileWriter = packageFileWriter; }

    void SetDirty(bool dirty)
    {
        if (mInMemFileNode)
        {
            mInMemFileNode->SetDirty(dirty);
        }
    }
    
public:
    Storage* mStorage;
    InMemPackageFileWriter* mPackageFileWriter;
    InMemFileNodePtr mInMemFileNode;
    bool mIsSynced;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemoryFile);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_IN_MEMORY_FILE_H
