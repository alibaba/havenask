#include <autil/StringUtil.h>
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/misc/exception.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index_define.h"
#include "indexlib/file_system/directory.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, IndexFormatVersion);

IndexFormatVersion::IndexFormatVersion(const string& version)
    : mIndexFormatVersion(version)
{
}

IndexFormatVersion::~IndexFormatVersion() 
{
}

bool IndexFormatVersion::Load(const DirectoryPtr& directory, bool mayNonExist)
{
    string content;
    if (!directory->LoadMayNonExist(INDEX_FORMAT_VERSION_FILE_NAME, content, mayNonExist))
    {
        return false;
    }
    *this = LoadFromString(content);
    return true;
}

bool IndexFormatVersion::Load(const string& path, bool mayNonExist)
{
    string content;
    if (!FileSystemWrapper::Load(path, content, mayNonExist))
    {
        return false;
    }
    *this = LoadFromString(content);
    return true;
}

IndexFormatVersion IndexFormatVersion::LoadFromString(const string& content)
{
    IndexFormatVersion formatVersion;
    try
    {
        FromJsonString(formatVersion, content); 
    }
    catch (const json::JsonParserError& e)
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "Parse index format version file FAILED,"
                             "content: %s", content.c_str());
    }
    return formatVersion;
}

void IndexFormatVersion::Store(const string& path)
{
    string jsonStr = ToJsonString(*this);
    IE_LOG(DEBUG, "Store index format file: [%s]", jsonStr.c_str());
    FileSystemWrapper::AtomicStore(path, jsonStr);
}

void IndexFormatVersion::Store(const DirectoryPtr& directory)
{
    string jsonStr = ToJsonString(*this);
    directory->Store(INDEX_FORMAT_VERSION_FILE_NAME, jsonStr, true);
}

void IndexFormatVersion::StoreBinaryVersion(const string& rootDir)
{
    IndexFormatVersion binaryVersion(INDEX_FORMAT_VERSION);
    string formatVersionPath = FileSystemWrapper::JoinPath(
            rootDir, INDEX_FORMAT_VERSION_FILE_NAME);

    binaryVersion.Store(formatVersionPath);
}

bool IndexFormatVersion::IsLegacyFormat() const
{
    // should checkCompatible first!
    vector<int32_t> binaryVersionIds;
    vector<int32_t> currentVersionIds;

    SplitVersionIds(INDEX_FORMAT_VERSION, binaryVersionIds);
    SplitVersionIds(mIndexFormatVersion, currentVersionIds);
    return binaryVersionIds[1] > currentVersionIds[1];
}

void IndexFormatVersion::SplitVersionIds(
        const string& versionStr, vector<int32_t>& versionIds) const
{
    vector<string> strVec = StringUtil::split(versionStr, ".", true);
    if (strVec.size() != 3)
    {
        INDEXLIB_FATAL_ERROR(
                IndexCollapsed, 
                "Index Format Version [%s] illegal.",
                versionStr.c_str());
    }
    versionIds.resize(3);

    for (size_t i = 0; i < strVec.size(); i++)
    {
        if (!StringUtil::strToInt32(strVec[i].c_str(), versionIds[i]))
        {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, 
                    "Index Format Version [%s] illegal.",
                    versionStr.c_str());
        }
    }

}

void IndexFormatVersion::CheckCompatible(const string& binaryVersion)
{
    vector<int32_t> binaryVersionIds;
    vector<int32_t> currentVersionIds;

    SplitVersionIds(binaryVersion, binaryVersionIds);
    SplitVersionIds(mIndexFormatVersion, currentVersionIds);
    
    if (binaryVersionIds[0] == currentVersionIds[0] && 
        (binaryVersionIds[1] == currentVersionIds[1] ||
         binaryVersionIds[1] - currentVersionIds[1] == 1))
    {
        return;
    }

    INDEXLIB_FATAL_ERROR(
            IndexCollapsed,
            "Index Format Version is incompatible with binary version. "
            "Current version [%s], Binary version [%s]. "
            "The first digits of both versions should be identical. "
            "The second digit of index version should be equal to"
            " or one less than that of current version",
            binaryVersion.c_str(),
            mIndexFormatVersion.c_str());
}

void IndexFormatVersion::Jsonize(JsonWrapper& json) 
{
    json.Jsonize("index_format_version", mIndexFormatVersion);
}

void IndexFormatVersion::Set(const string& version) 
{
    mIndexFormatVersion =  version;
}

bool IndexFormatVersion::operator == (const IndexFormatVersion& other) const
{
    return mIndexFormatVersion == other.mIndexFormatVersion;
}

bool IndexFormatVersion::operator != (const IndexFormatVersion& other) const
{
    return !(*this == other);
}

bool IndexFormatVersion::operator < (const IndexFormatVersion& other) const
{
    vector<int32_t> myVersionIds;
    vector<int32_t> otherVersionIds;

    SplitVersionIds(mIndexFormatVersion, myVersionIds);
    SplitVersionIds(other.mIndexFormatVersion, otherVersionIds);

    assert(myVersionIds.size() == otherVersionIds.size());

    size_t i = 0;
    while (i < myVersionIds.size() && myVersionIds[i] == otherVersionIds[i])
    {
        i++;
    }
    if (i < myVersionIds.size())
    {
        return myVersionIds[i] < otherVersionIds[i];
    }
    return false;
}

IE_NAMESPACE_END(index_base);

