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
#pragma once

#include <memory>

#include "autil/Lock.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/fslib/FenceContext.h"
#include "indexlib/index_base/index_meta/merge_task_resource.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(file_system, IFileSystem);

namespace indexlib { namespace index_base {

class MergeTaskResourceManager
{
public:
    typedef std::vector<MergeTaskResourcePtr> MergeTaskResourceVec;

public:
    MergeTaskResourceManager();
    ~MergeTaskResourceManager();

public:
    // init for declare from empty root
    void Init(const std::string& resourcePath,
              file_system::FenceContext* fenceContext = file_system::FenceContext::NoFence());

    // init from resource vector
    void Init(const MergeTaskResourceVec& resourceVec);

    // create new merge resource, return resource id
    resourceid_t DeclareResource(const char* data, size_t dataLen, const std::string& description = "");

    // get resource by resource id
    MergeTaskResourcePtr GetResource(resourceid_t resId) const;

    const MergeTaskResourceVec& GetResourceVec() const;

    auto GetTempWorkingDirectory()
    {
        if (mRootDir && mRootDir->IsExist("tmpWorking")) {
            return mRootDir->GetDirectory("tmpWorking", true);
        }
        return indexlib::file_system::DirectoryPtr();
    }

    auto CreateTempWorkingDirectory()
    {
        auto directory = GetTempWorkingDirectory();
        if (!directory) {
            static std::string annotation = "temp working path";
            (void)DeclareResource(annotation.data(), annotation.size());
            mRootDir->MakeDirectory("tmpWorking", indexlib::file_system::DirectoryOption())->Close();
        }
    }

    void CleanTempWorkingDirectory()
    {
        auto directory = GetTempWorkingDirectory();
        if (directory) {
            mRootDir->RemoveDirectory("tmpWorking", indexlib::file_system::RemoveOption());
        }
    }
    void Commit();

private:
    file_system::IFileSystemPtr CreateFileSystem(const std::string& rootPath, bool isOverride);

private:
    MergeTaskResourceVec mResourceVec;
    std::string mCommitPath;
    file_system::DirectoryPtr mRootDir;
    bool mIsPack;
    bool mIsDirty;
    mutable autil::ThreadMutex mLock;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergeTaskResourceManager);
}} // namespace indexlib::index_base
