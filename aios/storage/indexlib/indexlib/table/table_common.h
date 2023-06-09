/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef __INDEXLIB_TABLE_COMMON_H
#define __INDEXLIB_TABLE_COMMON_H

#include <memory>

#include "autil/legacy/jsonizable.h"
#include "fslib/fslib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/SegmentStatistics.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/memory_control/SimpleMemoryQuotaController.h"

DECLARE_REFERENCE_CLASS(index_base, SegmentDataBase);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(table, TableResource);

namespace indexlib { namespace table {

struct BuildSegmentDescription {
    BuildSegmentDescription() : isEntireDataSet(false), useSpecifiedDeployFileList(false) {}
    ~BuildSegmentDescription() {}
    bool isEntireDataSet;
    bool useSpecifiedDeployFileList;
    fslib::FileList deployFileList;
    indexlibv2::framework::SegmentStatistics segmentStats;
    bool AddIntegerStatistics(const std::string& key,
                              const indexlibv2::framework::SegmentStatistics::IntegerRangeType& range)
    {
        return segmentStats.AddStatistic(key, range);
    }
};
DEFINE_SHARED_PTR(BuildSegmentDescription);

struct TableWriterInitParam {
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
    MergeSegmentDescription() : docCount(0), useSpecifiedDeployFileList(false) {}
    ~MergeSegmentDescription() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool AddIntegerStatistics(const std::string& key,
                              const indexlibv2::framework::SegmentStatistics::IntegerRangeType& range)
    {
        return segmentStats.AddStatistic(key, range);
    }

public:
    int64_t docCount;
    bool useSpecifiedDeployFileList;
    fslib::FileList deployFileList;
    indexlibv2::framework::SegmentStatistics segmentStats;
};
DEFINE_SHARED_PTR(MergeSegmentDescription);
}} // namespace indexlib::table

#endif //__INDEXLIB_TABLE_COMMON_H
