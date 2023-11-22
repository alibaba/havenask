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

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/LogicalFileSystem.h"
#include "indexlib/index_define.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"

namespace indexlib { namespace index_base {

/*
    BranchFS is a fileSystem that is between logicalFileSystem and indexPartition, it provides a 'branch' feature like
   git repo which means you can make different branches in the same physical path, and also allow you to make a new
   branch based an existed branch, the new branch will inherit all files occur in the src branch, the delete operation
   to the files created by the src branch in the new branch will only delete the corresponding logical file which means
   the usage of the same files in the src branch will not be affected.

    I.E
    BranchFileSystem

                      branch1: file1 file2 file3

    then make branch2 based branch1:

    BranchFileSystem
                      branch1: file1 file2 file3

                      branch2: file1(inherited) file2(inherited) file3(inherited)

    then delete file2 in branch2:

    BranchFileSystem
                      branch1: file1 file2 file3

                      branch2: file1(inherited) file3(inherited)

    then add file2 in branch2:

    BranchFileSystem
                      branch1: file1 file2 file3

                      branch2: file1(inherited) file3(inherited) file2(not the file2 in branch1)

    User can use CommitToDefaultBranch and GetDirectoryFromDefaultBranch to make the MARKED_BRANCH or load the
   MARKED_BRANCH.

    see more usage in branch_fs_unittest.cpp.
*/

class BranchFS
{
public:
    /*
      this branch hinter is to select && store faster branch in mulit branches
     */
    class IBranchHinter
    {
    public:
        virtual ~IBranchHinter() {}

    public:
        // get the branch name
        virtual std::string GetNewBranchName(const std::string& rootPath) = 0;

        // choice the fast branch as the fork branch
        virtual bool SelectFastestBranch(const std::string& rootPath, std::string& branchName) = 0;

        // commit branch to default branch
        virtual void CommitToDefaultBranch(const std::string& rootPath, const std::string& branchName) = 0;

        // get default branch from root
        virtual std::string GetDefaultBranch(const std::string& root) = 0;

        // update count file if need
        virtual void UpdateCountFile(const std::string& root, const std::string& branchName) = 0;
    };
    DEFINE_SHARED_PTR(IBranchHinter);

public:
    static constexpr size_t MAX_BRANCH_ID = 1000;
    /*
       @param root: eg. dfs://ea120/mainse/generation_x/partition_x_x/
       @param branchName: eg. backup_1, branch_1
    */
    BranchFS(const std::string& root, const std::string& branchName);
    ~BranchFS();

    /*
         branchName is not equal to fileSystemName,  in most case user need not to specify fileSystemName.
         In fact, fileSystemName is only used in mergeFileSystem to keep some kind of thread safe stuffs now.
    */
    static std::unique_ptr<BranchFS> Create(const std::string& root, const std::string& branchName = "")
    {
        return std::unique_ptr<BranchFS>(new BranchFS(root, branchName));
    }

    static std::unique_ptr<BranchFS>
    CreateWithAutoFork(const std::string& root, const file_system::FileSystemOptions& fsOptions,
                       const util::MetricProviderPtr& metricProvider = util::MetricProviderPtr(),
                       IBranchHinter* hinter = nullptr);

    /*
         choice a given brach to fork.
    */
    static std::unique_ptr<BranchFS> Fork(const std::string& root, const std::string& srcBranchName,
                                          const std::string& branchName,
                                          const file_system::FileSystemOptions& fsOptions,
                                          const util::MetricProviderPtr& metricProvider = util::MetricProviderPtr());

    void Init(const util::MetricProviderPtr& metricProvider, const file_system::FileSystemOptions& fsOptions);
    file_system::DirectoryPtr GetRootDirectory() { return file_system::Directory::Get(mLogicalFS); }
    file_system::IFileSystemPtr GetFileSystem() { return mLogicalFS; };
    bool MountBranch(const std::string& root, const std::string& branchName, int32_t versionId,
                     file_system::FSMountType fsMountType);
    bool MountVersion(const std::string& root, const std::string& branchName, int32_t versionId,
                      file_system::FSMountType fsMountType);
    std::string GetBranchName() const { return mBranch; }
    std::string GetRootPath() const { return mRoot; }

    // mark this branch to be marked branch
    void CommitToDefaultBranch(IBranchHinter* hinter = nullptr);

    void UpdatePreloadDependence();

    bool IsLegacy() const { return mIsLegacy; }
    std::string GetBranchFSPhysicalRoot(const std::string& branchName) const;

    static bool IsBranchPath(const std::string& path);

    static std::string GetDefaultBranch(const std::string& root, IBranchHinter* hinter);

    static file_system::DirectoryPtr GetDirectoryFromDefaultBranch(const std::string& directoryRoot,
                                                                   const file_system::FileSystemOptions& fsOptions,
                                                                   IBranchHinter* hinter, std::string* epochId);

    static std::string GetBranchFSPhysicalRoot(const std::string& root, const std::string& branchName);

    // toremove
    static std::string TEST_GetBranchName(uint32_t branchId)
    {
        if (branchId == 0) {
            return "";
        }
        return BRANCH_DIR_NAME_PREFIX + std::to_string(branchId);
    }
    void TEST_MoveToMainRoad();
    static std::string TEST_EstimateFastestBranchName(const std::string& root);

private:
    /*
        to copy the unshared files such as checkpoints to branch root
    */
    void CopyUnSharedMetaFiles();

    /*
        to copy the checkpoints
    */
    void CopyCheckPoints();

    /*
         choice the fastest brach right now to fork.
    */
    static std::unique_ptr<BranchFS> AutoFork(const std::string& root, const std::string& branchName,
                                              const file_system::FileSystemOptions& fsOptions,
                                              const util::MetricProviderPtr& metricProvider = util::MetricProviderPtr(),
                                              IBranchHinter* hinter = nullptr);

    void CheckIsLegacy();
    void CommitPreloadDependence();

    static void GetSrcBranchFromHinter(IBranchHinter* hinter, const std::string& root, std::string& branchName);

private:
    file_system::IFileSystemPtr mLogicalFS;
    std::string mRoot;
    std::string mBranch;
    bool mIsLegacy;

    friend class BranchFsTest;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index_base
