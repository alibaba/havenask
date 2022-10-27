#include <autil/StringUtil.h>
#include "indexlib/file_system/versioned_package_file_meta.h"
#include "indexlib/file_system/file_system_define.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/util/path_util.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, VersionedPackageFileMeta);

VersionedPackageFileMeta::VersionedPackageFileMeta(int32_t versionId)
    : PackageFileMeta()
    , mVersionId(versionId)
{
}

VersionedPackageFileMeta::~VersionedPackageFileMeta() 
{
}

void VersionedPackageFileMeta::Jsonize(Jsonizable::JsonWrapper& json)
{
    PackageFileMeta::Jsonize(json);
}

void VersionedPackageFileMeta::Store(const string& root, const string& description)
{
    SortInnerFileMetas();
    ComputePhysicalFileLengths();
    const string& fileName = VersionedPackageFileMeta::GetPackageMetaFileName(description, mVersionId);
    string metaFilePath = PathUtil::JoinPath(root, fileName);
    IE_LOG(INFO, "store meta file[%s]", metaFilePath.c_str());
    string metaContent = ToString();
    FileSystemWrapper::AtomicStore(metaFilePath, metaContent.c_str(), metaContent.size());
}

void VersionedPackageFileMeta::Load(const string& metaFilePath)
{
    IE_LOG(INFO, "load meta file[%s]", metaFilePath.c_str());
    string metaContent;
    FileSystemWrapper::AtomicLoad(metaFilePath, metaContent);
    InitFromString(metaContent);
    mVersionId = VersionedPackageFileMeta::GetVersionId(
        PathUtil::GetFileName(metaFilePath));
}

void VersionedPackageFileMeta::Recognize(
    const string& description, int32_t recoverMetaId, const vector<string>& fileNames,
    set<string>& dataFileSet, set<string>& uselessMetaFileSet, string& recoverMetaPath)
{
    assert(dataFileSet.empty() && uselessMetaFileSet.empty());
    string dataPrefix = string(PACKAGE_FILE_PREFIX) + PACKAGE_FILE_DATA_SUFFIX + "." + description + ".";
    string metaPrefix = string(PACKAGE_FILE_PREFIX) + PACKAGE_FILE_META_SUFFIX + "." + description + ".";
    
    int32_t maxMetaId = -1;
    string maxMetaPath = "";
    recoverMetaPath = "";
    for (const string& fileName : fileNames)
    {
        if (StringUtil::startsWith(fileName, dataPrefix))
        {
            dataFileSet.insert(fileName);
        }
        else if (StringUtil::startsWith(fileName, metaPrefix))
        {
            uselessMetaFileSet.insert(fileName);
            int32_t metaId = GetVersionId(fileName);
            if (metaId < 0)
            {
                continue;
            }
            if (recoverMetaId == metaId)
            {
                recoverMetaPath = fileName;
            }
            if (metaId > maxMetaId)
            {
                maxMetaId = metaId;
                maxMetaPath = fileName;
            }
        }
        else
        {
            IE_LOG(DEBUG, "file [%s] can not match dataPrefix [%s] or metaPrefix [%s]",
                   fileName.c_str(), dataPrefix.c_str(), metaPrefix.c_str());
        }
    }
    if (recoverMetaId < 0)
    {
        recoverMetaPath = maxMetaPath;
    }
    uselessMetaFileSet.erase(recoverMetaPath);
}

int32_t VersionedPackageFileMeta::GetVersionId(const string& fileName)
{
    string versionIdStr = fileName.substr(fileName.rfind(".") + 1);
    int32_t versionId = -1;
    if (unlikely(!StringUtil::strToInt32(versionIdStr.c_str(), versionId)))
    {
        IE_LOG(WARN, "Invalid package file meta name [%s]", fileName.c_str());
        return -1;
    }
    return versionId;
}

string VersionedPackageFileMeta::GetDescription(const string& fileName)
{
    size_t endPos = fileName.rfind(".");
    size_t startPos = fileName.rfind(".", endPos - 1) + 1;
    return fileName.substr(startPos, endPos - startPos);
}

string VersionedPackageFileMeta::GetPackageDataFileName(
    const string& description, uint32_t dataFileIdx)
{
    return PackageFileMeta::GetPackageFileDataPath(
        PACKAGE_FILE_PREFIX, description, dataFileIdx);
}

string VersionedPackageFileMeta::GetPackageMetaFileName(
    const string& description, int32_t metaVersionId)
{
    return PackageFileMeta::GetPackageFileMetaPath(
        PACKAGE_FILE_PREFIX, description, metaVersionId);
}

IE_NAMESPACE_END(file_system);

