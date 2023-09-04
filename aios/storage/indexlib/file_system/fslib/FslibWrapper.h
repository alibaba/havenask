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

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <sys/types.h>

#include "autil/ThreadPool.h"
#include "fslib/common/common_type.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/fslib/DeleteOption.h"
#include "indexlib/file_system/fslib/IoConfig.h"

namespace autil {
class ThreadMutex;
}
// namespace autil
namespace fslib { namespace fs {
class MMapFile;
}} // namespace fslib::fs

namespace indexlib { namespace file_system {
class FslibFileWrapper;

class FslibWrapper
{
public:
    // for Fence interface
    static const std::string EPOCHID;
    static const std::string FENCE_INLINE_FILE;
    static const char FENCE_ARGS_SEP;

public:
    static FSResult<std::unique_ptr<FslibFileWrapper>>
    OpenFile(const std::string& fileName, fslib::Flag mode, bool useDirectIO = false, ssize_t fileLength = -1) noexcept;
    static FSResult<std::unique_ptr<fslib::fs::MMapFile>> MmapFile(const std::string& fileName, fslib::Flag openMode,
                                                                   char* start, int64_t length, int prot, int mapFlag,
                                                                   off_t offset, ssize_t fileLength) noexcept;
    static FSResult<void> DeleteFile(const std::string& path, const DeleteOption& deleteOption) noexcept;
    static FSResult<void> DeleteDir(const std::string& path, const DeleteOption& deleteOption) noexcept;

    static FSResult<bool> IsExist(const std::string& filePath) noexcept;
    static FSResult<bool> IsDir(const std::string& path) noexcept;       // false for not a dir, may not exist or file
    static FSResult<bool> IsFile(const std::string& path) noexcept;      // false for not a file, may not exist or dir
    static FSResult<bool> IsDirOrFile(const std::string& path) noexcept; // true for dir, false for file
    static FSResult<void> ListDirRecursive(const std::string& path, fslib::FileList& fileList) noexcept;
    static FSResult<void> ListDir(const std::string& dirPath, fslib::FileList& fileList) noexcept;
    static FSResult<void> ListDir(const std::string& dirPath, fslib::EntryList& fileList) noexcept;
    static FSResult<void> MkDir(const std::string& dirPath, bool recursive = false, bool mayExist = false) noexcept;
    static FSResult<void> MkDirIfNotExist(const std::string& dirPath) noexcept;
    static FSResult<void> Rename(const std::string& srcName, const std::string& dstName) noexcept;
    // only use to remove temp file to root, may lost file in hadoop and failOver scene
    static FSResult<void> RenameWithFenceContext(const std::string& srcName, const std::string& dstName,
                                                 FenceContext* fenceContext) noexcept;
    static FSResult<void> Copy(const std::string& srcName, const std::string& dstName) noexcept;
    // create a hard link to a file
    static FSResult<void> Link(const std::string& srcPath, const std::string& dstPath) noexcept;
    // create a symbolic link to a file
    static FSResult<void> SymLink(const std::string& srcPath, const std::string& dstPath) noexcept;
    static FSResult<size_t> GetFileLength(const std::string& fileName) noexcept;
    static FSResult<size_t> GetDirSize(const std::string& dirPath) noexcept;
    static FSResult<fslib::FileMeta> GetFileMeta(const std::string& fileName) noexcept;
    static FSResult<fslib::PathMeta> GetPathMeta(const std::string& path) noexcept;

public:
    static FSResult<void> AtomicStore(const std::string& filePath, const char* data, size_t len, bool removeIfExist,
                                      FenceContext* fenceContext) noexcept;
    static FSResult<void> AtomicStore(const std::string& filePath, const std::string& fileContent,
                                      bool removeIfExist = false, FenceContext* fenceContext = FenceContext::NoFence())
    {
        return AtomicStore(filePath, fileContent.data(), fileContent.size(), removeIfExist, fenceContext);
    }

    static FSResult<void> AtomicLoad(const std::string& filePath, std::string& fileContent) noexcept;
    static FSResult<void> AtomicLoad(FslibFileWrapper* file, std::string& fileContent) noexcept;

    static FSResult<void> Store(const std::string& filePath, const char* data, size_t len) noexcept;
    static FSResult<void> Store(const std::string& filePath, const std::string& fileContent)
    {
        return Store(filePath, fileContent.data(), fileContent.size());
    }
    static FSResult<void> Load(const std::string& filePath, std::string& fileContent) noexcept;

public:
    // exception when error, todo rm
    // IsExistE = IsExist().get()
    // IsDirE = IsDir().get()
    static void MkDirE(const std::string& dirPath, bool recursive = false, bool mayExist = false) noexcept(false);
    static void DeleteFileE(const std::string& path, const DeleteOption& deleteOption) noexcept(false);
    static void DeleteDirE(const std::string& path, const DeleteOption& deleteOption) noexcept(false);
    static void RenameE(const std::string& srcName, const std::string& dstName) noexcept(false);
    static void ListDirE(const std::string& dirPath, fslib::FileList& fileList) noexcept(false);
    static void AtomicStoreE(const std::string& filePath, const std::string& fileContent) noexcept(false);
    static void AtomicLoadE(const std::string& filePath, std::string& fileContent) noexcept(false);

public:
    static bool IsTempFile(const std::string& filePath) noexcept;
    static bool NeedMkParentDirBeforeOpen(const std::string& path_util) noexcept;
    static std::string JoinPath(const std::string& path, const std::string& name) noexcept;
    static void SetMergeIOConfig(const IOConfig& mergeIOConfig) noexcept { _mergeIOConfig = mergeIOConfig; }
    static const IOConfig& GetMergeIOConfig() noexcept { return _mergeIOConfig; }
    static void Reset() noexcept;
    static const autil::ThreadPoolPtr& GetWriteThreadPool() noexcept;
    static const autil::ThreadPoolPtr& GetReadThreadPool() noexcept;
    static std::string GetErrorString(fslib::ErrorCode ec) noexcept;
    static FSResult<void> UpdatePanguInlineFile(const std::string& path, const std::string& content) noexcept;
    static FSResult<void> UpdatePanguInlineFileCAS(const std::string& path, const std::string& oldContent,
                                                   const std::string& newContent) noexcept;
    static FSResult<void> CreatePanguInlineFile(const std::string& fileName) noexcept;
    static FSResult<void> StatPanguInlineFile(const std::string& fileName, std::string& out) noexcept;

    static FenceContext* CreateFenceContext(const std::string& fenceHintPath, const std::string& epochId) noexcept;
    static bool UpdateFenceInlineFile(FenceContext* fenceContext) noexcept;

public:
    // TODO: mv to private
    static FSResult<void> IsExist(const std::string& filePath, bool& ret) noexcept;
    static FSResult<void> GetFileLength(const std::string& fileName, int64_t& fileLength) noexcept;
    static FSResult<void> GetFileMeta(const std::string& fileName, fslib::FileMeta& fileMeta) noexcept;

private:
    static FSResult<void> OpenFile(const std::string& fileName, fslib::Flag mode, bool useDirectIO, ssize_t fileLength,
                                   FslibFileWrapper*& fileWrapper) noexcept;
    static FSResult<void> MmapFile(const std::string& fileName, fslib::Flag openMode, char* start, int64_t length,
                                   int prot, int mapFlag, off_t offset, ssize_t fileLength,
                                   fslib::fs::MMapFile*& mmapFile) noexcept;
    static FSResult<void> IsDir(const std::string& path,
                                bool& ret) noexcept; // false for not a dir, may not exist or file
    static FSResult<void> IsFile(const std::string& path,
                                 bool& ret) noexcept; // false for not a file, may not exist or dir
    static FSResult<void> GetPathMeta(const std::string& path, fslib::PathMeta& pathMeta) noexcept;
    static FSResult<void> GetDirSize(const std::string& path, size_t& dirSize) noexcept;
    static FSResult<void> Delete(const std::string& path, const DeleteOption& deleteOption) noexcept;
    static FSResult<void> Delete(const std::string& path, FenceContext* fenceContext) noexcept;
    static FSResult<void> DeleteFencing(const std::string& path, FenceContext* fenceContext) noexcept;
    static FSResult<void> RenameFencing(const std::string& srcPath, const std::string& destPath,
                                        FenceContext* fenceContext) noexcept;
    static FSResult<void> DeletePanguPathCAS(const std::string& path, const std::string& args) noexcept;
    static FSResult<void> RenamePanguPathCAS(const std::string& srcName, const std::string& args) noexcept;
    static void InitThreadPool(autil::ThreadPoolPtr& threadPool, uint32_t threadNum, uint32_t queueSize,
                               const std::string& name) noexcept;

private:
    static IOConfig _mergeIOConfig;
    // thread pool for async read/write
    static autil::ThreadPoolPtr _writePool;
    static autil::ThreadPoolPtr _readPool;
    static autil::ThreadMutex _writePoolLock;
    static autil::ThreadMutex _readPoolLock;

    // TODO: RM BEGIIN
private:
    static const uint32_t OPEN_ERROR = 0x01;
    static const uint32_t DELETE_FILE_ERROR = 0x02;
    static const uint32_t IS_EXIST_ERROR = 0x04;
    static const uint32_t LIST_DIR_ERROR = 0x08;
    static const uint32_t MAKE_DIR_ERROR = 0x10;
    static const uint32_t RENAME_ERROR = 0x20;
    static const uint32_t COPY_ERROR = 0x40;
    static const uint32_t GET_FILE_META_ERROR = 0x80;
    static const uint32_t MMAP_ERROR = 0x0100;
    static const uint32_t MMAP_LOCK_ONLY = 0x0200;
    static const uint32_t IS_EXIST_IO_ERROR = 0x0400;

    static void ClearError() {}
    static void SetError(uint32_t errorType, const std::string& fileName = ".*", uint32_t triggerTimes = 1) {}
    // TODO: RM END

private:
    AUTIL_LOG_DECLARE();
};
}} // namespace indexlib::file_system
