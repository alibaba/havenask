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
#ifndef __INDEXLIB_SEGMENT_DUMP_ITEM_H
#define __INDEXLIB_SEGMENT_DUMP_ITEM_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(index_base, SegmentWriter);
DECLARE_REFERENCE_CLASS(index_base, SegmentWriter);

namespace indexlib { namespace partition {

class SegmentDumpItem
{
public:
    SegmentDumpItem(const config::IndexPartitionOptions& options, const config::IndexPartitionSchemaPtr& schema,
                    const std::string& partitionName);
    virtual ~SegmentDumpItem();

public:
    virtual void Dump() = 0;
    // virtual for test
    virtual bool DumpWithMemLimit() = 0;
    virtual bool IsDumped() const = 0;
    virtual uint64_t GetInMemoryMemUse() const = 0;
    virtual uint64_t GetEstimateDumpSize() const = 0;
    virtual size_t GetTotalMemoryUse() const = 0;
    virtual segmentid_t GetSegmentId() const = 0;
    virtual index_base::SegmentInfoPtr GetSegmentInfo() const = 0;

protected:
    config::IndexPartitionOptions mOptions;
    config::IndexPartitionSchemaPtr mSchema;
    std::string mPartitionName;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentDumpItem);
}} // namespace indexlib::partition

#endif //__INDEXLIB_SEGMENT_DUMP_ITEM_H
