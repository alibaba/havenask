#include <autil/StringUtil.h>
#include "indexlib/file_system/directory.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/index_define.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/util/path_util.h"
#include "indexlib/misc/exception.h"
#include "indexlib/config/section_attribute_config.h"

using namespace std;
using namespace fslib;
using namespace autil;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, PartitionPatchIndexAccessor);

PartitionPatchIndexAccessor::PartitionPatchIndexAccessor()
    : mIsSub(false)
{
}

PartitionPatchIndexAccessor::~PartitionPatchIndexAccessor() 
{
}

void PartitionPatchIndexAccessor::Init(
        const DirectoryPtr& rootDir, const Version& version)
{
    mRootDir = rootDir;
    mVersion = version;
    if (version.GetSchemaVersionId() == DEFAULT_SCHEMAID ||
        version.GetVersionId() == INVALID_VERSION)
    {
        return;
    }

    string patchFileName =
        PartitionPatchMeta::GetPatchMetaFileName(version.GetVersionId());
    if (!rootDir->IsExist(patchFileName))
    {
        IE_LOG(INFO, "[%s] not exist in [%s]", patchFileName.c_str(),
               rootDir->GetPath().c_str());
        return;
    }
    mPatchMeta.Load(rootDir, version.GetVersionId());
}

DirectoryPtr PartitionPatchIndexAccessor::GetAttributeDirectory(
        segmentid_t segmentId, const string& attrName, bool throwExceptionIfNotExist)
{
    schemavid_t schemaId = mPatchMeta.GetSchemaIdByAttributeName(segmentId, attrName);
    if (schemaId == DEFAULT_SCHEMAID)
    {
        return DirectoryPtr();
    }
    
    string patchDirName = GetPatchRootDirName(schemaId);
    string segPath = PathUtil::JoinPath(patchDirName,
            mVersion.GetSegmentDirName(segmentId));
    if (mIsSub)
    {
        segPath = PathUtil::JoinPath(segPath, SUB_SEGMENT_DIR_NAME);
    }
    string attrDirPath = PathUtil::JoinPath(segPath, ATTRIBUTE_DIR_NAME);
    string attrPath = PathUtil::JoinPath(attrDirPath, attrName);
    IE_LOG(INFO, "attribute [%s] in segment [%d] in patch index path [%s]",
           attrName.c_str(), segmentId, attrPath.c_str());
    return mRootDir->GetDirectory(attrPath, throwExceptionIfNotExist);
}

DirectoryPtr PartitionPatchIndexAccessor::GetIndexDirectory(
        segmentid_t segmentId, const string& indexName, bool throwExceptionIfNotExist)
{
    schemavid_t schemaId = mPatchMeta.GetSchemaIdByIndexName(segmentId, indexName);
    if (schemaId == DEFAULT_SCHEMAID)
    {
        return DirectoryPtr();
    }

    string patchDirName = GetPatchRootDirName(schemaId);
    string segPath = PathUtil::JoinPath(patchDirName,
            mVersion.GetSegmentDirName(segmentId));
    if (mIsSub)
    {
        segPath = PathUtil::JoinPath(segPath, SUB_SEGMENT_DIR_NAME);
    }
    string indexDirPath = PathUtil::JoinPath(segPath, INDEX_DIR_NAME);
    string indexPath = PathUtil::JoinPath(indexDirPath, indexName);
    IE_LOG(INFO, "index [%s] in segment [%d] in patch index path [%s]",
           indexName.c_str(), segmentId, indexPath.c_str());
    return mRootDir->GetDirectory(indexPath, throwExceptionIfNotExist);
}

DirectoryPtr PartitionPatchIndexAccessor::GetSectionAttributeDirectory(
        segmentid_t segmentId, const string& indexName, bool throwExceptionIfNotExist)
{
    schemavid_t schemaId = mPatchMeta.GetSchemaIdByIndexName(segmentId, indexName);
    if (schemaId == DEFAULT_SCHEMAID)
    {
        return DirectoryPtr();
    }

    string patchDirName = GetPatchRootDirName(schemaId);
    string segPath = PathUtil::JoinPath(patchDirName,
            mVersion.GetSegmentDirName(segmentId));
    if (mIsSub)
    {
        segPath = PathUtil::JoinPath(segPath, SUB_SEGMENT_DIR_NAME);
    }
    string indexDirPath = PathUtil::JoinPath(segPath, INDEX_DIR_NAME);
    string sectionAttrName = SectionAttributeConfig::IndexNameToSectionAttributeName(indexName);
    string indexPath = PathUtil::JoinPath(indexDirPath, sectionAttrName);
    IE_LOG(INFO, "index section attribute [%s] in segment [%d] in patch index path [%s]",
           sectionAttrName.c_str(), segmentId, indexPath.c_str());
    return mRootDir->GetDirectory(indexPath, throwExceptionIfNotExist);
}

string PartitionPatchIndexAccessor::GetPatchRootDirName(schemavid_t schemaId)
{
    if (schemaId == DEFAULT_SCHEMAID)
    {
        INDEXLIB_FATAL_ERROR(BadParameter,
                             "schema id [%u] not valid for patch index", schemaId);
    }
    string dirName = string(PARTITION_PATCH_DIR_PREFIX) + "_";
    dirName += StringUtil::toString(schemaId);
    return dirName;
}

bool PartitionPatchIndexAccessor::ExtractSchemaIdFromPatchRootDir(
        const string& rootDir, schemavid_t& schemaId)
{
    string prefix = string(PARTITION_PATCH_DIR_PREFIX) + "_";
    if (rootDir.find(prefix) != 0)
    {
        return false;
    }
    string schemaIdStr = rootDir.substr(prefix.size());
    return StringUtil::fromString(schemaIdStr, schemaId);
}

void PartitionPatchIndexAccessor::ListPatchRootDirs(
        const DirectoryPtr& rootDir, FileList& patchRootList, bool localOnly)
{
    patchRootList.clear();
    FileList fileList;
    rootDir->ListFile("", fileList, false, false, localOnly);

    for (size_t i = 0; i < fileList.size(); i++)
    {
        schemavid_t schemaId;
        if (PartitionPatchIndexAccessor::ExtractSchemaIdFromPatchRootDir(
                        fileList[i], schemaId))
        {
            if (schemaId != DEFAULT_SCHEMAID)
            {
                patchRootList.push_back(fileList[i]);
            }
        }
    }
}

void PartitionPatchIndexAccessor::ListPatchRootDirs(
        const string& rootPath, FileList& patchRootList)
{
    patchRootList.clear();
    FileList fileList;
    FileSystemWrapper::ListDir(rootPath, fileList);

    for (size_t i = 0; i < fileList.size(); i++)
    {
        schemavid_t schemaId;
        if (PartitionPatchIndexAccessor::ExtractSchemaIdFromPatchRootDir(
                        fileList[i], schemaId))
        {
            if (schemaId != DEFAULT_SCHEMAID)
            {
                patchRootList.push_back(fileList[i]);
            }
        }
    }
}

IE_NAMESPACE_END(index_base);

