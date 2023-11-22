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
#pragma once

#include <cstdint>
#include <stddef.h>
#include <string>
#include <utility>
#include <vector>

#include "future_lite/Executor.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"
#include "indexlib/table/building_segment_reader.h"
#include "indexlib/table/segment_meta.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace indexlib { namespace table {

/*
    increase interface id when sub class interface has changed
    user should make sure that:
    sub class with different interface id should be in different namespaces

    eg:
    namespace N {
       class SubReader : public TableReader   // DEFAULT_INTERFACE_ID;
       {};
    }

    namespace N {
    namespace v1 {
       class SubReader : public TableReader
       {
          DECLARE_READER_INTERFACE_ID(1);
       };
    }
    }
*/

#define DECLARE_READER_INTERFACE_ID(id)                                                                                \
protected:                                                                                                             \
    void InitInterfaceId() override { mInterfaceId = id; }

class BuiltSegmentReader
{
public:
    BuiltSegmentReader() {}
    virtual ~BuiltSegmentReader() {}

public:
    bool Init(const table::SegmentMetaPtr& segMeta)
    {
        mSegMeta = segMeta;
        return Open(segMeta);
    }
    const table::SegmentMetaPtr& GetSegmentMeta() const { return mSegMeta; }

public:
    virtual bool Open(const table::SegmentMetaPtr& segMeta) { return false; }

protected:
    SegmentMetaPtr mSegMeta;
};
DEFINE_SHARED_PTR(BuiltSegmentReader);

class TableReader
{
public:
    TableReader();
    virtual ~TableReader();

public:
    bool Init(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
              future_lite::Executor* executor, const util::MetricProviderPtr& metricProvider);

    virtual bool DoInit();

    virtual bool Open(const std::vector<table::SegmentMetaPtr>& builtSegmentMetas,
                      const std::vector<BuildingSegmentReaderPtr>& dumpingSegments,
                      const BuildingSegmentReaderPtr& buildingSegmentReader, int64_t incVersionTimestamp) = 0;

    virtual size_t EstimateMemoryUse(const std::vector<table::SegmentMetaPtr>& builtSegmentMetas,
                                     const std::vector<BuildingSegmentReaderPtr>& dumpingSegments,
                                     const BuildingSegmentReaderPtr& buildingSegmentReader,
                                     int64_t incVersionTimestamp) const = 0;

    // interfaceid_t GetInterfaceId() const { return mInterfaceId; }
    void SetSegmentDependency(const std::vector<BuildingSegmentReaderPtr>& inMemSegments);
    virtual void SetForceSeekInfo(const std::pair<int64_t, int64_t>& forceSeekInfo) { mForceSeekInfo = forceSeekInfo; }

public:
    virtual bool SupportSegmentLevelReader() const { return false; }

    virtual BuiltSegmentReaderPtr CreateBuiltSegmentReader() const { return BuiltSegmentReaderPtr(); }

    virtual bool Open(const std::vector<BuiltSegmentReaderPtr>& builtSegments,
                      const std::vector<BuildingSegmentReaderPtr>& dumpingSegments,
                      const BuildingSegmentReaderPtr& buildingSegmentReader, int64_t incVersionTimestamp)
    {
        return false;
    }

    virtual BuiltSegmentReaderPtr GetSegmentReader(segmentid_t segId) const { return BuiltSegmentReaderPtr(); }

    virtual segmentid_t GetFirstNeedInMemSegId() const { return INVALID_SEGMENTID; }

public:
    virtual bool TEST_query(const std::vector<std::string>& conditions, std::vector<std::string>& result,
                            std::string& errorMsg)
    {
        errorMsg = "not supported";
        return false;
    }

protected:
    // virtual void InitInterfaceId() { mInterfaceId = DEFAULT_INTERFACE_ID; }

protected:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    // interfaceid_t mInterfaceId;
    std::pair<int64_t, int64_t> mForceSeekInfo;
    future_lite::Executor* mExecutor;
    util::MetricProviderPtr mMetricProvider;

private:
    std::vector<BuildingSegmentReaderPtr> mDependentInMemSegments;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TableReader);
}} // namespace indexlib::table
