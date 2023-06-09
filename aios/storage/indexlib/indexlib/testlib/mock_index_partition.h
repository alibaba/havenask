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

#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/partition_group_resource.h"
#include "unittest/unittest.h"

namespace indexlib { namespace testlib {

class MockIndexPartitionReader : public partition::IndexPartitionReader
{
public:
    MockIndexPartitionReader() : partition::IndexPartitionReader() {}

    MockIndexPartitionReader(const config::IndexPartitionSchemaPtr& schema) : partition::IndexPartitionReader(schema) {}

public:
    MOCK_METHOD(void, Open, (const index_base::PartitionDataPtr&), (override));

    MOCK_METHOD(const index::SummaryReaderPtr&, GetSummaryReader, (), (const, override));
    MOCK_METHOD(const index::SourceReaderPtr&, GetSourceReader, (), (const, override));
    MOCK_METHOD(const index::AttributeReaderPtr&, GetAttributeReader, (const std::string&), (const, override));
    MOCK_METHOD(const index::PackAttributeReaderPtr&, GetPackAttributeReader, (const std::string&), (const, override));
    MOCK_METHOD(const index::PackAttributeReaderPtr&, GetPackAttributeReader, (packattrid_t), (const, override));
    MOCK_METHOD(const index::AttributeReaderContainerPtr&, GetAttributeReaderContainer, (), (const, override));

    MOCK_METHOD(std::shared_ptr<index::InvertedIndexReader>, GetInvertedIndexReader, (), (const, override));
    MOCK_METHOD(const std::shared_ptr<index::InvertedIndexReader>&, GetInvertedIndexReader, (const std::string&),
                (const, override));
    MOCK_METHOD(const std::shared_ptr<index::InvertedIndexReader>&, GetInvertedIndexReader, (indexid_t),
                (const, override));

    MOCK_METHOD(const index::DeletionMapReaderPtr&, GetDeletionMapReader, (), (const, override));

    MOCK_METHOD(const index::PrimaryKeyIndexReaderPtr&, GetPrimaryKeyReader, (), (const, override));

    MOCK_METHOD(const index::KKVReaderPtr&, GetKKVReader, (regionid_t), (const, override));
    MOCK_METHOD(const index::KKVReaderPtr&, GetKKVReader, (const std::string&), (const, override));

    MOCK_METHOD(const index::KVReaderPtr&, GetKVReader, (regionid_t), (const, override));
    MOCK_METHOD(const index::KVReaderPtr&, GetKVReader, (const std::string&), (const, override));

    MOCK_METHOD(const table::TableReaderPtr&, GetTableReader, (), (const, override));

    MOCK_METHOD(index_base::Version, GetVersion, (), (const, override));
    MOCK_METHOD(index_base::Version, GetOnDiskVersion, (), (const, override));

    MOCK_METHOD(index::PartitionInfoPtr, GetPartitionInfo, (), (const, override));

    MOCK_METHOD(partition::IndexPartitionReaderPtr, GetSubPartitionReader, (), (const, override));

    MOCK_METHOD(bool, GetSortedDocIdRanges,
                (const std::vector<std::shared_ptr<table::DimensionDescription>>&, const DocIdRange&,
                 DocIdRangeVector&),
                (const, override));

    MOCK_METHOD(const AccessCounterMap&, GetIndexAccessCounters, (), (const, override));
    MOCK_METHOD(const AccessCounterMap&, GetAttributeAccessCounters, (), (const, override));
};
typedef ::testing::NiceMock<MockIndexPartitionReader> NiceMockIndexPartitionReader;
typedef ::testing::StrictMock<MockIndexPartitionReader> StrictMockIndexPartitionReader;

class MockIndexPartition;
typedef ::testing::NiceMock<MockIndexPartition> NiceMockIndexPartition;
typedef ::testing::StrictMock<MockIndexPartition> StrictMockIndexPartition;

class MockIndexPartition : public partition::IndexPartition
{
public:
    MockIndexPartition(const partition::IndexPartitionResource& res) : partition::IndexPartition(res) {}

public:
    static NiceMockIndexPartition* MakeNice()
    {
        auto res = partition::PartitionGroupResource::TEST_Create();
        return new NiceMockIndexPartition(partition::IndexPartitionResource(*res, "partition_name"));
    }
    static StrictMockIndexPartition* MakeStrict()
    {
        auto res = partition::PartitionGroupResource::TEST_Create();
        return new StrictMockIndexPartition(partition::IndexPartitionResource(*res, "partition_name"));
    }

public:
    MOCK_METHOD(partition::IndexPartition::OpenStatus, Open,
                (const std::string&, const std::string&, const config::IndexPartitionSchemaPtr&,
                 const config::IndexPartitionOptions&, versionid_t),
                (override));
    MOCK_METHOD(partition::IndexPartition::OpenStatus, ReOpen, (bool, versionid_t), (override));
    MOCK_METHOD(void, ReOpenNewSegment, (), (override));
    MOCK_METHOD(bool, CleanUnreferencedIndexFiles, (const std::set<std::string>&), (override));
    MOCK_METHOD(partition::PartitionWriterPtr, GetWriter, (), (const, override));
    MOCK_METHOD(partition::IndexPartitionReaderPtr, GetReader, (), (const, override));
    MOCK_METHOD(bool, NotAllowToModifyRootPath, (), (const, override));
    MOCK_METHOD(void, CheckParam, (const config::IndexPartitionSchemaPtr&, const config::IndexPartitionOptions&),
                (const, override));
};

}} // namespace indexlib::testlib
