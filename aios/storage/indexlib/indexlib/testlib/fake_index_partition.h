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
#ifndef __INDEXLIB_FAKE_INDX_PARTITION_H
#define __INDEXLIB_FAKE_INDX_PARTITION_H

#include "indexlib/common_define.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/testlib/fake_index_partition_writer.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/memory_control/MemoryReserver.h"

namespace indexlib { namespace testlib {

class FakeIndexPartition : public partition::OnlinePartition
{
public:
    FakeIndexPartition()
    {
        _getReaderTimes = 0;
        _writer.reset(new FakeIndexPartitionWriter(mOptions));
    }
    ~FakeIndexPartition() {}

private:
    FakeIndexPartition(const FakeIndexPartition&);
    FakeIndexPartition& operator=(const FakeIndexPartition&);

public:
    OpenStatus Open(const std::string& primaryDir, const std::string& secondaryDir,
                    const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                    versionid_t versionId = INVALID_VERSION) override
    {
        mReaderUpdater.reset(new partition::SearchReaderUpdater("default"));
        return OS_OK;
    }

    OpenStatus ReOpen(bool forceReopen, versionid_t reopenVersionId = INVALID_VERSION) override { return OS_OK; }

    void ReOpenNewSegment() override {}
    void Close() override {};
    partition::IndexPartitionReaderPtr GetReader() const override
    {
        ++_getReaderTimes;
        return _reader;
    }

    util::MemoryReserverPtr
    ReserveVirtualAttributesResource(const config::AttributeConfigVector& mainVirtualAttrConfigs,
                                     const config::AttributeConfigVector& subVirtualAttrConfigs) override
    {
        util::MemoryReserverPtr reserver(
            new util::MemoryReserver("", util::MemoryQuotaControllerCreator::CreateBlockMemoryController()));
        return reserver;
    }
    void AddVirtualAttributes(const config::AttributeConfigVector& mainVirtualAttrConfigs,
                              const config::AttributeConfigVector& subVirtualAttrConfigs) override
    {
    }

    partition::PartitionWriterPtr GetWriter() const override { return _writer; }

    uint32_t GetReaderCount() const { return 1; }

public:
    void setIndexPartitionReader(const partition::IndexPartitionReaderPtr& reader) { _reader = reader; }
    size_t getReaderTimes() const { return _getReaderTimes; }
    void setSchema(const std::string& root, const std::string& schemaFileName)
    {
        mSchema = index_base::SchemaAdapter::TEST_LoadSchema(util::PathUtil::JoinPath(root, schemaFileName));
    }

public:
    config::AttributeConfigVector _mainVirtualAttrConfigs;
    config::AttributeConfigVector _subVirtualAttrConfigs;

private:
    partition::IndexPartitionReaderPtr _reader;
    partition::PartitionWriterPtr _writer;
    mutable size_t _getReaderTimes;
};

DEFINE_SHARED_PTR(FakeIndexPartition);
}} // namespace indexlib::testlib

#endif //__INDEXLIB_FAKE_INDX_PARTITION_H
