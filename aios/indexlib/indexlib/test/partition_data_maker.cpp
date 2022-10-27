#include <autil/StringUtil.h>
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/on_disk_partition_data.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/config/field_config.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(testlib);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(test);
IE_LOG_SETUP(test, PartitionDataMaker);

PartitionDataMaker::PartitionDataMaker() 
{
}

PartitionDataMaker::~PartitionDataMaker() 
{
}

segmentid_t PartitionDataMaker::MakeSegmentData(
        const DirectoryPtr& partitionDirectory, 
        const string& segmentInfoStr, bool isMergedSegment)
{
    vector<int64_t> segInfos;
    StringUtil::fromString(segmentInfoStr, segInfos, ",");

    segmentid_t segmentId = segInfos[0];

    SegmentInfo segmentInfo;
    if (segInfos.size() > 1)
    {
        segmentInfo.docCount = segInfos[1];
    }
    else
    {
        segmentInfo.docCount = 1;
    }
    if (segInfos.size() > 2)
    {
        segmentInfo.locator = document::Locator(
                StringUtil::toString(segInfos[2]));
    }
    segmentInfo.mergedSegment = isMergedSegment;

    if (segInfos.size() > 3)
    {
        segmentInfo.timestamp = segInfos[3];
    }
    else
    {
        segmentInfo.timestamp = 0;
    }

    if (segInfos.size() > 4)
    {
        segmentInfo.schemaId = segInfos[4];
    }
    else
    {
        segmentInfo.schemaId = DEFAULT_SCHEMAID;
    }

    Version version;
    string segDirName = version.GetNewSegmentDirName(segmentId);
    DirectoryPtr segmentDirectory = partitionDirectory->MakeDirectory(segDirName);
    segmentInfo.Store(segmentDirectory);
    if (segmentInfo.docCount > 0)
    {
        segmentDirectory->MakeDirectory(DELETION_MAP_DIR_NAME);
    }
    return segmentId;
}

void PartitionDataMaker::MakePartitionDataFiles(
        versionid_t versionId, int64_t timestamp,
        const DirectoryPtr& partitionDirectory, 
        const string& segmentInfoStrs)
{
    vector<string> segmentInfoStrVec;
    Version version(versionId, timestamp);
    StringUtil::fromString(segmentInfoStrs, segmentInfoStrVec, ";");
    for (size_t i = 0; i < segmentInfoStrVec.size(); i++)
    {
        segmentid_t segId = MakeSegmentData(partitionDirectory, segmentInfoStrVec[i]);
        version.AddSegment(segId);
    }
    version.Store(partitionDirectory, true);

    if (!partitionDirectory->IsExist(INDEX_FORMAT_VERSION_FILE_NAME))
    {
        IndexFormatVersion formatVersion(INDEX_FORMAT_VERSION);
        formatVersion.Store(partitionDirectory);
    }
}

map<versionid_t, Version> PartitionDataMaker::MakeVersions(
    const DirectoryPtr& partitionDir, const string& versionsInfoStr,
    IndexPartitionSchemaPtr schema)
{
    if (!schema)
    {
        string field = "string1:string;";
        string index = "pk:primarykey64:string1";
        schema = SchemaMaker::MakeSchema(field, index, "", "");
    }
    PartitionStateMachine psm;
    psm.Init(schema, IndexPartitionOptions(), partitionDir->GetPath());
    stringstream addCmdSS;
    for (docid_t docId = 0; docId < 3; ++docId)
    {
        addCmdSS << "cmd=add";
        for (auto it = schema->GetFieldSchema()->Begin(); it != schema->GetFieldSchema()->End(); ++it)
        {
            addCmdSS << "," << (*it)->GetFieldName() << "=" << docId;
        }
        addCmdSS << ";";
    }
    psm.Transfer(BUILD_FULL_NO_MERGE, addCmdSS.str(), "", "");
    assert(!partitionDir->IsExist("version.1"));
    partitionDir->RemoveFile("version.0");
    partitionDir->RemoveFile("deploy_meta.0");
    
    map<versionid_t, Version> versions;
    for (const string versionInfoStr : StringUtil::split(versionsInfoStr, ";"))
    {
        auto tmp = StringUtil::split(versionInfoStr, ":");
        versionid_t versionId = StringUtil::fromString<uint32_t>(tmp[0]);
        Version version(versionId, 1576120000 + versionId);
        
        for (const string& segIdStr : StringUtil::split(tmp[1], ","))
        {
            segmentid_t segId = StringUtil::fromString<uint32_t>(segIdStr);
            string segName = version.GetNewSegmentDirName(segId);
            if (!partitionDir->IsExist(segName))
            {
                FileSystemWrapper::Copy(partitionDir->GetPath() + "/segment_0_level_0",
                                        partitionDir->GetPath() + "/" + segName);
            }
            version.AddSegment(segId);
            version.Store(partitionDir, true);
            DeployIndexWrapper::DumpDeployMeta(partitionDir, version);
            versions[versionId] = version;
        }
    }
    return versions;
}

PartitionDataPtr PartitionDataMaker::CreatePartitionData(
        const IndexlibFileSystemPtr& fileSystem,
        IndexPartitionOptions options,
        IndexPartitionSchemaPtr schema)
{
    if (!schema)
    {
        string field = "string1:string;";
        string index = "pk:primarykey64:string1";    
        schema = SchemaMaker::MakeSchema(field, index, "", "");
    }

    DirectoryPtr rootDirectory = DirectoryCreator::Get(
            fileSystem, fileSystem->GetRootPath(), true);
    if (!rootDirectory->IsExist(INDEX_FORMAT_VERSION_FILE_NAME))
    {
        IndexFormatVersion formatVersion(INDEX_FORMAT_VERSION);
        formatVersion.Store(rootDirectory);
    }

    DumpSegmentContainerPtr dumpSegContainer(new DumpSegmentContainer);
    PartitionDataPtr partitionData = PartitionDataCreator::CreateBuildingPartitionData(
            fileSystem, schema, options,
            util::MemoryQuotaControllerCreator::CreatePartitionMemoryController(), dumpSegContainer);
    return partitionData;
}

OnDiskPartitionDataPtr PartitionDataMaker::CreateSimplePartitionData(
        const IndexlibFileSystemPtr& fileSystem, 
        const string& rootDir, Version version, bool hasSubSegment)
{
    string rootPath = rootDir;
    if (rootPath.empty())
    {
        rootPath = fileSystem->GetRootPath();
    }

    DirectoryPtr rootDirectory = DirectoryCreator::Get(
            fileSystem, rootPath, true);
    if (!rootDirectory->IsExist(INDEX_FORMAT_VERSION_FILE_NAME))
    {
        IndexFormatVersion formatVersion(INDEX_FORMAT_VERSION);
        formatVersion.Store(rootDirectory);
    }

    OnDiskPartitionDataPtr partitionData = PartitionDataCreator::CreateOnDiskPartitionData(
            fileSystem, version, rootDir, hasSubSegment);
    return partitionData;
}

IE_NAMESPACE_END(test);

