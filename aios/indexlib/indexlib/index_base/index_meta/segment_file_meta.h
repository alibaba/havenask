#ifndef __INDEXLIB_SEGMENT_FILE_META_H
#define __INDEXLIB_SEGMENT_FILE_META_H

#include <tr1/memory>
#include <fslib/fslib.h>
#include "indexlib/indexlib.h"
#include <fslib/fslib.h>
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index_base, SegmentFileMeta);

IE_NAMESPACE_BEGIN(index_base);

class IndexFileList;

class SegmentFileMeta
{
public:
    SegmentFileMeta();
    ~SegmentFileMeta();

public:
    static SegmentFileMetaPtr Create(const file_system::DirectoryPtr& segDir, bool isSub);
    bool Load(const file_system::DirectoryPtr& segDir, bool isSub);

public:
    void ListFile(const std::string& dirPath,
                  fslib::FileList& fileList,
                  bool recursive = false) const;

    void ListFile(const std::string& dirPath,
                  std::vector<std::pair<std::string, int64_t>>& entryList,
                  bool recursive = false) const;

    int64_t CalculateDirectorySize(const std::string& dirPath);

    bool IsExist(const std::string& filePath) const;

    size_t GetFileLength(const std::string& fileName, bool ignoreNoEntry = true) const;

    bool HasPackageFile() const { return mHasPackageFile; }

public:
    void TEST_Print();

private:
    bool Load(const file_system::DirectoryPtr& segDirectory, IndexFileList& meta);
    bool LoadSub(const file_system::DirectoryPtr& segDirectory, IndexFileList& meta);
    bool LoadPackageFileMeta(const file_system::DirectoryPtr& segDirectory, bool appendSubName);
    bool IsSubDirectory(const std::string& parentDir, const std::string& path,
                        bool recursive, std::string& subDir) const;
private:
    std::map<std::string, fslib::FileMeta> mFileMetas;
    std::string mRoot;
    bool mHasPackageFile;
    bool mIsSub;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentFileMeta);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_SEGMENT_FILE_META_H
