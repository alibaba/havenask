#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/on_disk_partition_data.h"

namespace indexlib { namespace test {

class PartitionDataMaker
{
public:
    PartitionDataMaker();
    ~PartitionDataMaker();

public:
    // segmentInfoStr=segmentId,docCount,locator,timestamp,schemavid
    static segmentid_t MakeSegmentData(const file_system::DirectoryPtr& partitionDirectory,
                                       const std::string& segmentInfoStr, bool isMergedSegment = false);

    // segmentInfoStrs=segmentId,docCount,locator,timestamp;segmentId...
    static void MakePartitionDataFiles(versionid_t versionId, int64_t timestamp,
                                       const file_system::DirectoryPtr& partitionDirectory,
                                       const std::string& segmentInfoStrs);

    // versionInfoStr=versionId:segId1,segId2;versionId:segId1,segId2
    static std::map<versionid_t, index_base::Version>
    MakeVersions(const file_system::DirectoryPtr& partitionDirectory, const std::string& versionInfoStr,
                 config::IndexPartitionSchemaPtr schema = config::IndexPartitionSchemaPtr());

    static index_base::PartitionDataPtr
    CreatePartitionData(const file_system::IFileSystemPtr& fileSystem,
                        config::IndexPartitionOptions options = config::IndexPartitionOptions(),
                        config::IndexPartitionSchemaPtr schema = config::IndexPartitionSchemaPtr());

    static partition::OnDiskPartitionDataPtr
    CreateSimplePartitionData(const file_system::IFileSystemPtr& fileSystem, const std::string& rootDir = "",
                              index_base::Version version = index_base::Version(), bool hasSubSegment = false);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionDataMaker);
}} // namespace indexlib::test
