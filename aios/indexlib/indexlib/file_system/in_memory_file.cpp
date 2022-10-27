#include "indexlib/file_system/in_memory_file.h"
#include "indexlib/file_system/storage.h"
#include "indexlib/file_system/in_mem_package_file_writer.h"

using namespace std;
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, InMemoryFile);

InMemoryFile::InMemoryFile(
    const string& filePath, uint64_t fileLen, Storage* storage,
    const BlockMemoryQuotaControllerPtr& memController)
    : mStorage(storage)
    , mPackageFileWriter(NULL)
    , mIsSynced(false)
{
    assert(mStorage);
    mInMemFileNode.reset(new InMemFileNode(fileLen, false, LoadConfig(), memController));
    mInMemFileNode->Open(filePath, FSOT_IN_MEM);
    mInMemFileNode->Resize(fileLen);
    mInMemFileNode->SetDirty(true);
    mStorage->StoreWhenNonExist(mInMemFileNode);
}

InMemoryFile::~InMemoryFile() {}

void InMemoryFile::Truncate(size_t newLength)
{
    mStorage->Truncate(mInMemFileNode->GetPath(), newLength);
    mInMemFileNode->Resize(newLength);
}

void InMemoryFile::Sync(const FSDumpParam& param)
{
    if (mIsSynced)
    {
        return;
    }
    mIsSynced = true;
    if (mPackageFileWriter)
    {
        mPackageFileWriter->StoreInMemoryFile(mInMemFileNode, param);
        mInMemFileNode.reset();
    }
    else
    {
        mStorage->StoreFile(mInMemFileNode, param);
    }
}



IE_NAMESPACE_END(file_system);

