#include <autil/StringUtil.h>
#include "indexlib/file_system/package_file_meta.h"
#include "indexlib/file_system/file_node.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/util/path_util.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, PackageFileMeta);


PackageFileMeta::InnerFileMeta::InnerFileMeta(const string& path, bool isDir)
    : mFilePath(path)
    , mOffset(0)
    , mLength(0)
    , mFileIdx(0)
    , mIsDir(isDir)
{
}    

void PackageFileMeta::InnerFileMeta::Init(size_t offset, size_t length, uint32_t fileIdx)
{
    mOffset = offset;
    mLength = length;
    mFileIdx = fileIdx;
}
    
bool PackageFileMeta::InnerFileMeta::operator ==(const PackageFileMeta::InnerFileMeta& other) const
{
    return mFilePath == other.mFilePath
        && mOffset == other.mOffset
        && mLength == other.mLength
        && mFileIdx == other.mFileIdx
        && mIsDir == other.mIsDir;
}

void PackageFileMeta::InnerFileMeta::Jsonize(JsonWrapper& json)
{
    json.Jsonize("path", mFilePath);
    json.Jsonize("offset", mOffset);
    json.Jsonize("length", mLength);
    json.Jsonize("fileIdx", mFileIdx);
    json.Jsonize("isDir", mIsDir);
}

//////////////////////////////////////////////////////////////////////

PackageFileMeta::PackageFileMeta()
    : mFileAlignSize(getpagesize())
    , mMetaStrLen(-1)
{
}

PackageFileMeta::~PackageFileMeta() 
{
}

void PackageFileMeta::InitFromString(const std::string& metaContent)
{
    autil::legacy::FromJsonString(*this, metaContent);
    // compatity problem:
    // when old binary read a new package_meta file, 'meta.ToString().size()' will get a wrong length which resulting reopen fail.
    mMetaStrLen = metaContent.size();
}

void PackageFileMeta::InitFromFileNode(
        const string& packageFilePath,
        const vector<FileNodePtr>& fileNodes,
        size_t fileAlignSize)
{
    mFileAlignSize = fileAlignSize;
    string dirPath = PathUtil::GetParentDirPath(packageFilePath);

    size_t curFileOffset = 0;
    for (size_t i = 0; i < fileNodes.size(); i++)
    {
        string fileRelativePath = GetRelativeFilePath(dirPath, 
                fileNodes[i]->GetPath());

        if (fileNodes[i]->GetType() == FSFT_DIRECTORY)
        {
            InnerFileMeta innerFileMeta(fileRelativePath, true);
            innerFileMeta.Init(0, 0, 0);
            mFileMetaVec.push_back(innerFileMeta);
        }
        else
        {
            assert(fileNodes[i]->GetType() == FSFT_IN_MEM);
            InnerFileMeta innerFileMeta(fileRelativePath, false);
            innerFileMeta.Init(curFileOffset, fileNodes[i]->GetLength(), 0);
            curFileOffset += (fileNodes[i]->GetLength() + mFileAlignSize - 1)
                             / mFileAlignSize * mFileAlignSize;
            mFileMetaVec.push_back(innerFileMeta);
        }
    }
    string packageFileDataPath = GetPackageFileDataPath(packageFilePath, "", 0);
    AddPhysicalFile(util::PathUtil::GetFileName(packageFileDataPath), "");
    ComputePhysicalFileLengths();
}

void PackageFileMeta::AddInnerFile(InnerFileMeta innerFileMeta)
{
    mFileMetaVec.push_back(innerFileMeta);
}

bool PackageFileMeta::Load(const string& dir, bool mayNonExist)
{
    string content;
    string metaPath = GetPackageFileMetaPath(PathUtil::JoinPath(dir, PACKAGE_FILE_PREFIX));
    if (!FileSystemWrapper::Load(metaPath, content, mayNonExist))
    {
        return false;
    }
    InitFromString(content);
    return true;
}

void PackageFileMeta::Store(const string& dir)
{
    SortInnerFileMetas();
    ComputePhysicalFileLengths();
    string metaPath = GetPackageFileMetaPath(PathUtil::JoinPath(dir, PACKAGE_FILE_PREFIX));
    FileSystemWrapper::AtomicStore(metaPath, ToString());
}

string PackageFileMeta::GetRelativeFilePath(
        const string& parentPath, const string& absPath)
{
    if (parentPath == absPath)
    {
        return string("");
    }

    string normalizeDirPath = FileSystemWrapper::NormalizeDir(parentPath);
    if (absPath.find(normalizeDirPath) != 0)
    {
        INDEXLIB_FATAL_ERROR(InconsistentState, "Invalid inner file path [%s]",
                             absPath.c_str());
    }
    string relativePath = absPath.substr(normalizeDirPath.size());
    return relativePath;
}

void PackageFileMeta::SortInnerFileMetas()
{
    auto compare = [](const InnerFileMeta& left, const InnerFileMeta& right) {
        // <idx, offset, dir, length, path>
        // dir always has offset=0, length=0
        assert(!left.IsDir() || (left.GetLength() == 0 && left.GetOffset() == 0));
        assert(!right.IsDir() || (right.GetLength() == 0 && right.GetOffset() == 0));

        if (left.GetDataFileIdx() != right.GetDataFileIdx())
        {
            return left.GetDataFileIdx() < right.GetDataFileIdx();
        }
        if (left.GetOffset() != right.GetOffset())
        {
            return left.GetOffset() < right.GetOffset();
        }
        if (left.IsDir() != right.IsDir())
        {
            return left.IsDir();
        }
        if (left.GetLength() != right.GetLength())
        {
            return left.GetLength() < right.GetLength();
        }
        return left.GetFilePath() < right.GetFilePath();
    };
    sort(mFileMetaVec.begin(), mFileMetaVec.end(), compare);
}

void PackageFileMeta::ComputePhysicalFileLengths()
{
    mPhysicalFileLengths.resize(mPhysicalFileNames.size(), 0);
    for (const auto& fileMeta : mFileMetaVec)
    {
        if (fileMeta.IsDir())
        {
            continue;
        }
        assert(fileMeta.GetLength() >= 0);
        size_t length = fileMeta.GetOffset() + fileMeta.GetLength();
        uint32_t dataFileIdx = fileMeta.GetDataFileIdx();
        if (length > mPhysicalFileLengths[dataFileIdx])
        {
            mPhysicalFileLengths[dataFileIdx] = length;
        }
    }
}

void PackageFileMeta::Jsonize(JsonWrapper& json)
{
    json.Jsonize("inner_files", mFileMetaVec, mFileMetaVec);
    json.Jsonize("file_align_size", mFileAlignSize, mFileAlignSize);
    json.Jsonize("physical_file_names", mPhysicalFileNames, mPhysicalFileNames);
    json.Jsonize("physical_file_lengths", mPhysicalFileLengths, mPhysicalFileLengths);
    json.Jsonize("physical_file_tags", mPhysicalFileTags, mPhysicalFileTags);
    if (json.GetMode() == FROM_JSON)
    {
        // for compatible
        if (mPhysicalFileNames.empty())
        {
            uint32_t maxDataFileIdx = 0;
            bool hasFile = false;
            for (const auto& fileMeta : mFileMetaVec)
            {
                if (!fileMeta.IsDir())
                {
                    hasFile = true;
                }
                if (fileMeta.GetDataFileIdx() > maxDataFileIdx)
                {
                    maxDataFileIdx = fileMeta.GetDataFileIdx();
                }
            }
            if (hasFile)
            {
                for (uint32_t i = 0; i < maxDataFileIdx + 1; ++i)
                {
                    string packageFileDataPath = GetPackageFileDataPath(PACKAGE_FILE_PREFIX, "", i);
                    AddPhysicalFile(util::PathUtil::GetFileName(packageFileDataPath), "");
                }
                ComputePhysicalFileLengths();
            }
        }
        if (unlikely(mPhysicalFileNames.size() != mPhysicalFileLengths.size() ||
                     mPhysicalFileNames.size() != mPhysicalFileTags.size()))
        {
            INDEXLIB_FATAL_ERROR(InconsistentState,
                    "physical_file_names[%lu], physical_file_lengths[%lu], physical_file_tags[%lu]",
                    mPhysicalFileNames.size(), mPhysicalFileLengths.size(), mPhysicalFileTags.size());
        }
    }
}

string PackageFileMeta::GetPackageFileDataPath(
    const string& packageFilePath, const string& description, uint32_t dataFileIdx)
{
    if (description.empty())
    {
        // eg: package_file.__data__0, FOR Build
        return packageFilePath + PACKAGE_FILE_DATA_SUFFIX + 
            StringUtil::toString(dataFileIdx);
    }
    // eg: package_file.__data__.DESCRIPTION.0, FOR Merge
    return packageFilePath + PACKAGE_FILE_DATA_SUFFIX + 
        "." + description + "." + StringUtil::toString(dataFileIdx);
}

string PackageFileMeta::GetPackageFileMetaPath(
    const string& packageFilePath, const string& description, int32_t metaVersionId)
{
    if (description.empty())
    {
        // this is final meta name
        // eg: package_file.__meta__, FOR Build & EndMerge
        return packageFilePath + PACKAGE_FILE_META_SUFFIX; 
    }
    
    // eg: package_file.__meta__.DESCRIPTION.VERSION, FOR DoMerge
    return packageFilePath + PACKAGE_FILE_META_SUFFIX + "." + description +
        "." + StringUtil::toString(metaVersionId);
}

int32_t PackageFileMeta::GetPackageDataFileIdx(const std::string& packageFileDataPath)
{
    string suffix = PACKAGE_FILE_DATA_SUFFIX;
    size_t pos = packageFileDataPath.rfind(suffix);
    if (pos == string::npos)
    {
        return -1;
    }
    pos += suffix.size();
    int32_t dataFileIdx = -1;
    if (StringUtil::fromString(packageFileDataPath.substr(pos), dataFileIdx))
    {
        return dataFileIdx;
    }
    return -1;
}

void PackageFileMeta::AddPhysicalFile(const string& name, const string& tag)
{
    mPhysicalFileNames.push_back(name);
    mPhysicalFileTags.push_back(tag);
}

void PackageFileMeta::AddPhysicalFiles(const vector<string>& names, const vector<string> tags)
{
    mPhysicalFileNames.insert(mPhysicalFileNames.end(), names.begin(), names.end());
    mPhysicalFileTags.insert(mPhysicalFileTags.end(), tags.begin(), tags.end());
}

size_t PackageFileMeta::GetMetaStrLength() const
{
    if (mMetaStrLen == -1)
    {
        return ToString().size();
    }
    return mMetaStrLen;
}

IE_NAMESPACE_END(file_system);
