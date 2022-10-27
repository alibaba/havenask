#include <autil/StringUtil.h>
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_define.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/util/path_util.h"

using namespace std;
using namespace fslib;
using namespace autil;

IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, VersionLoader);

bool VersionLoader::MatchPattern(const string& fileName, const string& prefix, char sep)
{
    if (fileName.find(prefix) != 0)
    {
        return false;
    }
    size_t prefixLen = prefix.length() + 1;
    if (fileName.length() < prefixLen || fileName[prefixLen - 1] != sep)
    {
        return false;
    }
    string lftVerSufix = fileName.substr(prefixLen);
    int32_t lftVal;
    return StringUtil::strToInt32(lftVerSufix.c_str(), lftVal);
}

bool VersionLoader::IsValidVersionFileName(const string& fileName)
{
    return MatchPattern(fileName, VERSION_FILE_NAME_PREFIX, '.');
}

bool VersionLoader::IsValidSegmentFileName(const string& fileName)
{
    return (Version::IsValidSegmentDirName(fileName, false)
            || Version::IsValidSegmentDirName(fileName, true));
}

static bool IsNotVersionFile(const string& fileName)
{
    return !VersionLoader::IsValidVersionFileName(fileName);
}

static bool IsNotSegmentFile(const string& fileName)
{
    return !VersionLoader::IsValidSegmentFileName(fileName);
}

struct VersionFileComp
{
    bool operator () (const string& lft, const string& rht)
    {
        int32_t lftVal = VersionLoader::extractSuffixNumber(lft,
                VERSION_FILE_NAME_PREFIX);
        int32_t rhtVal = VersionLoader::extractSuffixNumber(rht,
                VERSION_FILE_NAME_PREFIX);
        return lftVal < rhtVal;
    }
};

struct SegmentDirComp
{
    bool operator () (const string& lft, const string& rht)
    {
        int32_t lftVal = Version::GetSegmentIdByDirName(lft);
        int32_t rhtVal = Version::GetSegmentIdByDirName(rht);
        return lftVal < rhtVal;
    }
};

VersionLoader::VersionLoader() 
{
}

VersionLoader::~VersionLoader() 
{
}

void VersionLoader::ListVersion(const string& rootDir, FileList &fileList)
{
    std::string normRootDir = storage::FileSystemWrapper::NormalizeDir(rootDir);    
    FileSystemWrapper::ListDir(normRootDir, fileList);
    FileList::iterator it = remove_if(fileList.begin(),
            fileList.end(), IsNotVersionFile);

    fileList.erase(it, fileList.end());
    
    sort(fileList.begin(), fileList.end(), VersionFileComp());
}

void VersionLoader::ListVersion(const DirectoryPtr& directory,
                                fslib::FileList &fileList, bool localOnly)
{
    directory->ListFile("", fileList, false, false, localOnly || NeedSkipSecondaryRoot(directory->GetPath()));
    FileList::iterator it = remove_if(fileList.begin(),
            fileList.end(), IsNotVersionFile);

    fileList.erase(it, fileList.end());
    
    sort(fileList.begin(), fileList.end(), VersionFileComp());
}

void VersionLoader::GetVersion(const string& root, Version& version,
                               versionid_t versionId)
{
    string rootDir = root;
    if (*(rootDir.rbegin()) != '/')
    {
        rootDir += '/';
    }
    if (versionId == INVALID_VERSION) 
    {
        fslib::FileList fileList;

        ListVersion(rootDir, fileList);

        if (fileList.size() != 0)
        {
            string versionFullPath = rootDir + fileList[fileList.size() - 1];
            GetVersion(versionFullPath, version);
        }
        return;
    }
    string versionFilePath = rootDir + Version::GetVersionFileName(versionId);
    GetVersion(versionFilePath, version);
}

void VersionLoader::GetVersion(const DirectoryPtr& directory,
                               Version &version, versionid_t versionId)
{
    if (versionId == INVALID_VERSION)
    {
        fslib::FileList fileList;
        ListVersion(directory, fileList);
        if (fileList.size() != 0)
        {
            string versionPath = fileList[fileList.size() - 1];
            version.Load(directory, versionPath);
        }
        return;
    }

    string versionFileName = Version::GetVersionFileName(versionId);
    if (!version.Load(directory, versionFileName))
    {
        string absPath = PathUtil::JoinPath(directory->GetPath(), versionFileName);
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "expected version file[%s] lost",
                             absPath.c_str());
    }
}

void VersionLoader::GetVersion(const string& versionFilePath, Version& version)
{
    string versionContent;
    FileSystemWrapper::AtomicLoad(versionFilePath, versionContent);
    version.FromString(versionContent);
}

void VersionLoader::ListSegments(const string& rootDir,
                                 fslib::FileList &fileList)
{
    FileSystemWrapper::ListDir(rootDir, fileList);
    FileList::iterator it = remove_if(fileList.begin(),
            fileList.end(), IsNotSegmentFile);
    fileList.erase(it, fileList.end());

    sort(fileList.begin(), fileList.end(), SegmentDirComp());
}

void VersionLoader::ListSegments(const file_system::DirectoryPtr& directory,
                                 fslib::FileList &fileList, bool localOnly)
{
    directory->ListFile("", fileList, false, false, localOnly || NeedSkipSecondaryRoot(directory->GetPath()));
    FileList::iterator it = remove_if(fileList.begin(),
            fileList.end(), IsNotSegmentFile);
    fileList.erase(it, fileList.end());

    sort(fileList.begin(), fileList.end(), SegmentDirComp());
}

versionid_t VersionLoader::GetVersionId(const string& versionFileName)
{
    return (versionid_t)extractSuffixNumber(versionFileName,
            VERSION_FILE_NAME_PREFIX);
}

int32_t VersionLoader::extractSuffixNumber(const string& name, 
        const string& prefix)
{
    string tmpName = name;
    int32_t prefixLen = prefix.length() + 1;
    string suffix = tmpName.substr(prefixLen);        
    int32_t retVal = -1;
    bool ret = StringUtil::strToInt32(suffix.c_str(), retVal);
    if (!ret)
    {
        INDEXLIB_FATAL_ERROR(InvalidVersion, "Invalid pattern name: [%s]",
                             tmpName.c_str());
    } 
    return retVal;
}

bool VersionLoader::NeedSkipSecondaryRoot(const std::string& rootDir)
{
    return (StringUtil::endsWith(rootDir, RT_INDEX_PARTITION_DIR_NAME)
            || StringUtil::endsWith(rootDir, JOIN_INDEX_PARTITION_DIR_NAME));
}

IE_NAMESPACE_END(index_base);


