#include "indexlib/partition/deploy_index_validator.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/index_base/patch/partition_patch_meta.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/index_file_list.h"
#include "indexlib/index_base/index_meta/segment_file_list_wrapper.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index_define.h"

using namespace std;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, DeployIndexValidator);

void DeployIndexValidator::ValidateDeploySegments(
    const DirectoryPtr& rootDir, const Version& version)
{
    IE_LOG(INFO, "Begin validate path [%s], version [%d]",
           rootDir->GetPath().c_str(), version.GetVersionId());
    assert(rootDir);
    Version::Iterator it = version.CreateIterator();
    while (it.HasNext())
    {
        segmentid_t curSegId = it.Next();
        string segDirName = version.GetSegmentDirName(curSegId);
        ValidateSingleSegmentDeployFiles(rootDir->GetDirectory(segDirName, true));
    }
    ValidatePartitionPatchSegmentDeployFiles(rootDir, version);
    IE_LOG(INFO, "End validate path [%s], version [%d]",
           rootDir->GetPath().c_str(), version.GetVersionId());
}

void DeployIndexValidator::ValidateSingleSegmentDeployFiles(const DirectoryPtr& segDir)
{
    assert(segDir);
    IndexFileList indexFileList;
    if (!SegmentFileListWrapper::Load(segDir, indexFileList))
    {
        INDEXLIB_FATAL_ERROR(FileIO, "deploy index file is missing in segment[%s]",
                             segDir->GetPath().c_str());
    }
    for (const FileInfo& fileInfo : indexFileList.deployFileMetas)
    {
        segDir->Validate(fileInfo.filePath, fileInfo.fileLength);
    }
}

void DeployIndexValidator::ValidatePartitionPatchSegmentDeployFiles(
        const DirectoryPtr& rootDir, const Version& version)
{
    if (version.GetSchemaVersionId() == DEFAULT_SCHEMAID)
    {
        return;
    }

    PartitionPatchMeta patchMeta;
    patchMeta.Load(rootDir, version.GetVersionId());
    PartitionPatchMeta::Iterator metaIter = patchMeta.CreateIterator();
    while (metaIter.HasNext())
    {
        SchemaPatchInfoPtr schemaInfo = metaIter.Next();
        assert(schemaInfo);
        SchemaPatchInfo::Iterator sIter = schemaInfo->Begin();
        for (; sIter != schemaInfo->End(); sIter++)
        {
            const SchemaPatchInfo::PatchSegmentMeta& segMeta = *sIter;
            if (!version.HasSegment(segMeta.GetSegmentId()))
            {
                continue;
            }
            string patchSegPath = FileSystemWrapper::JoinPath(
                    PartitionPatchIndexAccessor::GetPatchRootDirName(schemaInfo->GetSchemaId()),
                    version.GetSegmentDirName(segMeta.GetSegmentId()));
            ValidateSingleSegmentDeployFiles(rootDir->GetDirectory(patchSegPath, true));
        }
    }
}

IE_NAMESPACE_END(partition);

