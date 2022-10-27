#ifndef __INDEXLIB_TABLE_COMMON_H
#define __INDEXLIB_TABLE_COMMON_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/util/memory_control/simple_memory_quota_controller.h"

#include <autil/legacy/jsonizable.h>
#include <fslib/fslib.h>

DECLARE_REFERENCE_CLASS(index_base, SegmentDataBase);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(table, TableResource);

IE_NAMESPACE_BEGIN(table);

struct BuildSegmentDescription
{
    BuildSegmentDescription()
    : isEntireDataSet(false)
    , useSpecifiedDeployFileList(false) {}
    ~BuildSegmentDescription() {}
    bool isEntireDataSet;
    bool useSpecifiedDeployFileList;
    fslib::FileList deployFileList;
};
DEFINE_SHARED_PTR(BuildSegmentDescription);

struct TableWriterInitParam
{
    config::IndexPartitionSchemaPtr schema;
    config::IndexPartitionOptions options;
    file_system::DirectoryPtr rootDirectory;
    index_base::SegmentDataBasePtr newSegmentData;
    file_system::DirectoryPtr segmentDataDirectory;
    index_base::Version onDiskVersion;
    TableResourcePtr tableResource;
    util::UnsafeSimpleMemoryQuotaControllerPtr memoryController;
    PartitionRange partitionRange;
};
DEFINE_SHARED_PTR(TableWriterInitParam);

class MergeSegmentDescription : public autil::legacy::Jsonizable
{
public:
    MergeSegmentDescription()
    : docCount(0)
    , useSpecifiedDeployFileList(false) {}
    ~MergeSegmentDescription() {}
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override; 
public:    
    int64_t docCount;
    bool useSpecifiedDeployFileList;
    fslib::FileList deployFileList;
};
DEFINE_SHARED_PTR(MergeSegmentDescription);

IE_NAMESPACE_END(table);

#endif //__INDEXLIB_TABLE_COMMON_H
