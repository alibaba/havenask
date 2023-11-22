#include "indexlib/test/partition_data_maker.h"

#include "autil/StringUtil.h"
#include "indexlib/config/field_config.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/partition/building_partition_data.h"
#include "indexlib/partition/on_disk_partition_data.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/test/directory_creator.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"

using namespace std;
using namespace autil;

using namespace indexlib::file_system;
using namespace indexlib::index;
using namespace indexlib::partition;
using namespace indexlib::index_base;
using namespace indexlib::config;

using namespace indexlib::util;
namespace indexlib { namespace test {
IE_LOG_SETUP(test, PartitionDataMaker);

PartitionDataMaker::PartitionDataMaker() {}

PartitionDataMaker::~PartitionDataMaker() {}

segmentid_t PartitionDataMaker::MakeSegmentData(const DirectoryPtr& partitionDirectory, const string& segmentInfoStr,
                                                bool isMergedSegment)
{
    vector<int64_t> segInfos;
    StringUtil::fromString(segmentInfoStr, segInfos, ",");

    segmentid_t segmentId = segInfos[0];

    SegmentInfo segmentInfo;
    if (segInfos.size() > 1) {
        segmentInfo.docCount = segInfos[1];
    } else {
        segmentInfo.docCount = 1;
    }
    if (segInfos.size() > 2) {
        segmentInfo.SetLocator(indexlibv2::framework::Locator(0, segInfos[2]));
    }
    segmentInfo.mergedSegment = isMergedSegment;

    if (segInfos.size() > 3) {
        segmentInfo.timestamp = segInfos[3];
    } else {
        segmentInfo.timestamp = 0;
    }

    if (segInfos.size() > 4) {
        segmentInfo.schemaId = segInfos[4];
    } else {
        segmentInfo.schemaId = DEFAULT_SCHEMAID;
    }

    Version version;
    string segDirName = version.GetNewSegmentDirName(segmentId);
    DirectoryPtr segmentDirectory = partitionDirectory->MakeDirectory(segDirName);
    segmentInfo.Store(segmentDirectory);
    if (segmentInfo.docCount > 0) {
        segmentDirectory->MakeDirectory(DELETION_MAP_DIR_NAME);
    }
    return segmentId;
}

void PartitionDataMaker::MakePartitionDataFiles(versionid_t versionId, int64_t timestamp,
                                                const DirectoryPtr& partitionDirectory, const string& segmentInfoStrs)
{
    vector<string> segmentInfoStrVec;
    Version version(versionId, timestamp);
    StringUtil::fromString(segmentInfoStrs, segmentInfoStrVec, ";");
    for (size_t i = 0; i < segmentInfoStrVec.size(); i++) {
        segmentid_t segId = MakeSegmentData(partitionDirectory, segmentInfoStrVec[i]);
        version.AddSegment(segId);
    }
    version.Store(partitionDirectory, true);

    if (!partitionDirectory->IsExist(INDEX_FORMAT_VERSION_FILE_NAME)) {
        IndexFormatVersion formatVersion(INDEX_FORMAT_VERSION);
        formatVersion.Store(partitionDirectory);
    }
}

map<versionid_t, Version> PartitionDataMaker::MakeVersions(const DirectoryPtr& rootDir, const string& versionsInfoStr,
                                                           IndexPartitionSchemaPtr schema)
{
    if (!schema) {
        string field = "string1:string;";
        string index = "pk:primarykey64:string1";
        schema = SchemaMaker::MakeSchema(field, index, "", "");
    }
    PartitionStateMachine psm;
    psm.Init(schema, IndexPartitionOptions(), rootDir->GetOutputPath());
    stringstream addCmdSS;
    for (docid_t docId = 0; docId < 3; ++docId) {
        addCmdSS << "cmd=add";
        const auto& fieldConfigs = schema->GetFieldConfigs();
        for (const auto& fieldConfig : fieldConfigs) {
            addCmdSS << "," << fieldConfig->GetFieldName() << "=" << docId;
        }
        addCmdSS << ";";
    }
    psm.Transfer(BUILD_FULL_NO_MERGE, addCmdSS.str(), "", "");

    std::string partitionPath = rootDir->GetOutputPath();
    assert(!FslibWrapper::IsExist(partitionPath + "/version.1").GetOrThrow());
    THROW_IF_FS_ERROR(FslibWrapper::DeleteFile(partitionPath + "/version.0", DeleteOption::NoFence(false)).Code(),
                      "delete [%s/version.0] failed", partitionPath.c_str());
    THROW_IF_FS_ERROR(FslibWrapper::DeleteFile(partitionPath + "/deploy_meta.0", DeleteOption::NoFence(false)).Code(),
                      "delete [%s/deploy_meta.0] failed", partitionPath.c_str());

    map<versionid_t, Version> versions;
    for (const string& versionInfoStr : StringUtil::split(versionsInfoStr, ";")) {
        auto tmp = StringUtil::split(versionInfoStr, ":");
        versionid_t versionId = StringUtil::fromString<uint32_t>(tmp[0]);
        Version version(versionId, 1576120000 + versionId);

        for (const string& segIdStr : StringUtil::split(tmp[1], ",")) {
            segmentid_t segId = StringUtil::fromString<uint32_t>(segIdStr);
            string segName = version.GetNewSegmentDirName(segId);
            if (!FslibWrapper::IsExist(PathUtil::JoinPath(partitionPath, segName)).GetOrThrow()) {
                if (FslibWrapper::Copy(partitionPath + "/segment_0_level_0", partitionPath + "/" + segName).Code() !=
                    FSEC_OK) {
                    INDEXLIB_FATAL_ERROR(FileIO, "copy file failed");
                }
            }
            version.AddSegment(segId);
            // version.Store(partitionDir, true);
            DeployIndexWrapper::DumpDeployMeta(partitionPath, version);
            versions[versionId] = version;
        }
        version.TEST_Store(partitionPath, true);
    }
    return versions;
}

PartitionDataPtr PartitionDataMaker::CreatePartitionData(const IFileSystemPtr& fileSystem,
                                                         IndexPartitionOptions options, IndexPartitionSchemaPtr schema)
{
    if (!schema) {
        string field = "string1:string;";
        string index = "pk:primarykey64:string1";
        schema = SchemaMaker::MakeSchema(field, index, "", "");
    }

    DirectoryPtr rootDirectory = Directory::Get(fileSystem);
    if (!rootDirectory->IsExist(INDEX_FORMAT_VERSION_FILE_NAME)) {
        IndexFormatVersion formatVersion(INDEX_FORMAT_VERSION);
        formatVersion.Store(rootDirectory);
    }

    DumpSegmentContainerPtr dumpSegContainer(new DumpSegmentContainer);
    util::MetricProviderPtr metricProvider;
    util::CounterMapPtr counterMap;
    plugin::PluginManagerPtr pluginManager;
    auto memoryController = util::MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    BuildingPartitionParam param(options, schema, memoryController, dumpSegContainer, counterMap, pluginManager,
                                 metricProvider, document::SrcSignature());

    PartitionDataPtr partitionData = PartitionDataCreator::CreateBuildingPartitionData(
        param, fileSystem, index_base::Version(INVALID_VERSIONID), "", index_base::InMemorySegmentPtr());
    return partitionData;
}

OnDiskPartitionDataPtr PartitionDataMaker::CreateSimplePartitionData(const IFileSystemPtr& fileSystem,
                                                                     const string& rootDir, Version version,
                                                                     bool hasSubSegment)
{
    string rootPath = rootDir;
    DirectoryPtr rootDirectory = DirectoryCreator::Get(fileSystem, rootPath, true);
    if (!rootDirectory->IsExist(INDEX_FORMAT_VERSION_FILE_NAME)) {
        IndexFormatVersion formatVersion(INDEX_FORMAT_VERSION);
        formatVersion.Store(rootDirectory);
    }

    OnDiskPartitionDataPtr partitionData =
        OnDiskPartitionData::CreateOnDiskPartitionData(fileSystem, version, rootDir, hasSubSegment);
    return partitionData;
}
}} // namespace indexlib::test
