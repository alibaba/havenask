/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/index_base/merge_task_resource_manager.h"

#include "autil/EnvUtil.h"
#include "indexlib/file_system/FenceDirectory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"
// #include "indexlib/file_system/package_storage.h"
#include "indexlib/file_system/Directory.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;
using namespace indexlib::file_system;
namespace indexlib { namespace index_base {

IE_LOG_SETUP(index_base, MergeTaskResourceManager);

MergeTaskResourceManager::MergeTaskResourceManager() : mIsDirty(false)
{
    mIsPack = autil::EnvUtil::getEnv("INDEXLIB_PACKAGE_MERGE_META", false);
}

MergeTaskResourceManager::~MergeTaskResourceManager() { Commit(); }

void MergeTaskResourceManager::Commit()
{
    if (!mIsDirty) {
        return;
    }
    assert(mRootDir);
    mRootDir->Sync(true);
    FenceContextPtr fenceContext = mRootDir->GetFenceContext();
    if (fenceContext) {
        auto ec = FslibWrapper::DeleteDir(mCommitPath, DeleteOption::Fence(fenceContext.get(), true)).Code();
        THROW_IF_FS_ERROR(ec, "commit failed due to failed deletion");

        ec = FslibWrapper::RenameWithFenceContext(mRootDir->GetOutputPath(), mCommitPath, fenceContext.get()).Code();
        THROW_IF_FS_ERROR(ec, "commit failed due to failed renaming");
    }
    mIsDirty = false;
}

IFileSystemPtr MergeTaskResourceManager::CreateFileSystem(const string& rootPath, bool isOverride)
{
    FileSystemOptions fsOptions = FileSystemOptions::OfflineWithBlockCache(nullptr);
    fsOptions.outputStorage = mIsPack ? FSST_PACKAGE_MEM : FSST_DISK;
    fsOptions.useCache = true;
    return FileSystemCreator::Create("MergeTaskResourceManager", rootPath, fsOptions, nullptr, isOverride,
                                     FenceContext::NoFence())
        .GetOrThrow();
}

// init for write
void MergeTaskResourceManager::Init(const std::string& resourcePath, FenceContext* fenceContext)
{
    mCommitPath = resourcePath;
    string workPath = resourcePath;
    if (fenceContext) {
        PathUtil::TrimLastDelim(workPath);
        workPath = workPath + "." + fenceContext->epochId + TEMP_FILE_SUFFIX;
    }
    mRootDir = Directory::Get(CreateFileSystem(workPath, true));
}

// init for read
void MergeTaskResourceManager::Init(const MergeTaskResourceVec& resourceVec)
{
    string rootPath = "";
    for (resourceid_t id = 0; id < (resourceid_t)resourceVec.size(); id++) {
        auto resource = resourceVec[id];
        if (!resource || !resource->IsValid()) {
            INDEXLIB_FATAL_ERROR(BadParameter, "NULL or invalid MergeTaskResource in param!");
        }

        if (rootPath.empty()) {
            rootPath = resource->GetRootPath();
            IFileSystemPtr fs = CreateFileSystem(rootPath, false);
            mRootDir = Directory::Get(fs);
            mRootDir->MountPackage();
        }
        // copy merge task resource
        {
            ScopedLock lock(mLock);
            mResourceVec.push_back(resourceVec[id]);
            mResourceVec[id]->SetRoot(mRootDir);
        }
        assert(resource->GetDataLen() ==
               mRootDir->GetFileLength(MergeTaskResource::GetResourceFileName(resource->GetResourceId())));
        if (resource->GetRootPath() != rootPath) {
            INDEXLIB_FATAL_ERROR(BadParameter, "not same rootPath [%s:%s]!", rootPath.c_str(),
                                 resource->GetRootPath().c_str());
        }
        if (resource->GetResourceId() != id) {
            INDEXLIB_FATAL_ERROR(BadParameter, "resource id not match [%d:%d]!", id, resource->GetResourceId());
        }
    }
}

const MergeTaskResourceManager::MergeTaskResourceVec& MergeTaskResourceManager::GetResourceVec() const
{
    ScopedLock lock(mLock);
    return mResourceVec;
}

resourceid_t MergeTaskResourceManager::DeclareResource(const char* data, size_t dataLen, const string& description)
{
    if (!mRootDir) {
        INDEXLIB_FATAL_ERROR(UnSupported, "root path for merge resource should not be empty!");
    }
    MergeTaskResourcePtr resource;
    {
        ScopedLock lock(mLock);
        resource.reset(new MergeTaskResource(mRootDir, (resourceid_t)mResourceVec.size()));
        mResourceVec.push_back(resource);
        mIsDirty = true;
    }
    resource->Store(data, dataLen);
    resource->SetDescription(description);
    // TODO: @qingran package
    // if (mIsPack)
    // {
    //     mRootDir->GetFileSystem()->GetPackageStorage()->Sync();
    // }
    return resource->GetResourceId();
}

MergeTaskResourcePtr MergeTaskResourceManager::GetResource(resourceid_t resId) const
{
    ScopedLock lock(mLock);
    if (resId < 0 || resId >= (resourceid_t)mResourceVec.size()) {
        return MergeTaskResourcePtr();
    }

    MergeTaskResourcePtr resource = mResourceVec[resId];
    if (resource && resource->GetResourceId() != resId) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "resource Id [%d:%d] not match!", resId, resource->GetResourceId());
    }
    return resource;
}
}} // namespace indexlib::index_base
