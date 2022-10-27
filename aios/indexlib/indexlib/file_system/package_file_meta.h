#ifndef __INDEXLIB_PACKAGE_FILE_META_H
#define __INDEXLIB_PACKAGE_FILE_META_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "autil/legacy/jsonizable.h"

DECLARE_REFERENCE_CLASS(file_system, FileNode);
IE_NAMESPACE_BEGIN(file_system);

class PackageFileMeta : public autil::legacy::Jsonizable
{
public:
    class InnerFileMeta : public autil::legacy::Jsonizable
    {
    public:
        InnerFileMeta(const std::string& path = "", bool isDir = false);
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
        void Init(size_t offset, size_t length, uint32_t fileIdx);
        const std::string& GetFilePath() const { return mFilePath; }
        size_t GetOffset() const { return mOffset; }
        size_t GetLength() const { return mLength; }
        bool IsDir() const { return mIsDir; }
        uint32_t GetDataFileIdx() const { return mFileIdx; }
        void SetDataFileIdx(uint32_t fileIdx) { mFileIdx = fileIdx; }
        void SetFilePath(const std::string& filePath) { mFilePath = filePath; }
        bool operator ==(const InnerFileMeta& other) const;
        bool operator <(const InnerFileMeta& other) const { return mFilePath < other.mFilePath; }
    private:
        std::string mFilePath;
        size_t mOffset;
        size_t mLength;
        uint32_t mFileIdx;
        bool mIsDir;
    };

public:
    typedef std::vector<InnerFileMeta> InnerFileMetaVec;
    typedef InnerFileMetaVec::const_iterator Iterator;
    
public:
    PackageFileMeta();
    ~PackageFileMeta();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    
public:
    void InitFromString(const std::string& metaContent);
    
    // only for build
    void InitFromFileNode(const std::string& packageFilePath,
                          const std::vector<FileNodePtr>& fileNodes,
                          size_t fileAlignSize);
    
    void AddInnerFile(InnerFileMeta innerFileMeta);

    bool Load(const std::string& dir, bool mayNonExist = false);
    void Store(const std::string& dir);
    
    std::string ToString() const 
    { return autil::legacy::ToJsonString(*this); }

    Iterator Begin() const
    { return mFileMetaVec.begin(); }

    Iterator End() const
    { return mFileMetaVec.end(); }

    size_t GetFileAlignSize() const 
    { return mFileAlignSize; }
    
    void SetFileAlignSize(size_t fileAlignSize)
    { mFileAlignSize = fileAlignSize; }
    
    size_t GetInnerFileCount() const
    { return mFileMetaVec.size(); }

    // TODO, add const std::string& tag 
    void AddPhysicalFile(const std::string& name, const std::string& tag);
    void AddPhysicalFiles(const std::vector<std::string>& names, const std::vector<std::string> tags);

    const std::vector<std::string>& GetPhysicalFileNames() const { return mPhysicalFileNames; }
    const std::vector<std::string>& GetPhysicalFileTags() const { return mPhysicalFileTags; }
    const std::vector<size_t>& GetPhysicalFileLengths() const { return mPhysicalFileLengths; }

    const std::string& GetPhysicalFileName(uint32_t dataFileIdx) const
    {
        assert(dataFileIdx < mPhysicalFileNames.size());
        return mPhysicalFileNames[dataFileIdx];
    }
    const std::string& GetPhysicalFileTag(uint32_t dataFileIdx) const
    {
        assert(dataFileIdx < mPhysicalFileTags.size());
        return mPhysicalFileTags[dataFileIdx];
    }
    size_t GetPhysicalFileLength(uint32_t dataFileIdx) const
    {
        if (unlikely(mPhysicalFileLengths.empty()))
        {
            const_cast<PackageFileMeta*>(this)->ComputePhysicalFileLengths();
        }
        assert(dataFileIdx < mPhysicalFileLengths.size());
        return mPhysicalFileLengths[dataFileIdx];
    }

    size_t GetMetaStrLength() const;

public:
    static std::string GetPackageFileDataPath(
        const std::string& packageFilePath, const std::string& description,
        uint32_t dataFileIdx);
    
    static std::string GetPackageFileMetaPath(
        const std::string& packageFilePath,
        const std::string& description = "", int32_t metaVersionId = -1);

    static int32_t GetPackageDataFileIdx(const std::string& packageFileDataPath);

protected:
    void SortInnerFileMetas();
    void ComputePhysicalFileLengths();

private:
    std::string GetRelativeFilePath(const std::string& parentPath, 
                                    const std::string& absPath);

private:
    std::vector<std::string> mPhysicalFileNames;
    std::vector<std::string> mPhysicalFileTags;
    std::vector<size_t> mPhysicalFileLengths;
    InnerFileMetaVec mFileMetaVec;
    size_t mFileAlignSize;
    int64_t mMetaStrLen;
private:
    friend class PackageFileMetaTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackageFileMeta);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_PACKAGE_FILE_META_H
