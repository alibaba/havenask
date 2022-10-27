#ifndef __INDEXLIB_INDEX_FILE_LIST_H
#define __INDEXLIB_INDEX_FILE_LIST_H

#include <tr1/memory>
#include <functional>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file_system_define.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(index_base);

typedef file_system::FileInfo FileInfo;

class IndexFileList : public autil::legacy::Jsonizable
{
public:
    IndexFileList() {};
    ~IndexFileList() {};

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void FromString(const std::string& indexMetaStr);
    std::string ToString() const;

    bool Load(const std::string& path, bool* exist = NULL);
    bool Load(const file_system::DirectoryPtr& directory, const std::string& fileName,
              bool* exist = NULL);

    void Append(const FileInfo& fileMeta, bool isFinal = false);
    void AppendFinal(const FileInfo& fileMeta);

    void Sort();
    // be equivalent to self = set(lhs) - set(rhs)
    // ATTENTION: lhs and rhs will change by sort
    void FromDifference(IndexFileList& lhs, IndexFileList& rhs);

    void Filter(std::function<bool(const FileInfo&)> predicate);

    std::string GetFileName() const;
    
public:
    typedef std::vector<FileInfo> FileInfoVec;
    FileInfoVec deployFileMetas;
    FileInfoVec finalDeployFileMetas;
    std::string lifecycle;
    std::string mFileName;
    bool isComplete = false; // true when all file has fileLength and modifyTime

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexFileList);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_INDEX_FILE_LIST_H
