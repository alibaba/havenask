#include "catalog/entity/Partition.h"

#include "gtest/gtest.h"
#include <string>

#include "catalog/util/ProtoUtil.h"

namespace catalog {

class PartitionTest : public ::testing::Test {};

auto createSimplePartitionProto() {
    proto::Partition proto;
    proto.set_partition_name("part1");
    proto.set_table_name("tb1");
    proto.set_database_name("db1");
    proto.set_catalog_name("ct1");
    proto.set_partition_type(proto::PartitionType::TABLE_PARTITION);
    proto.set_version(5);
    return proto;
}

auto createFullPartitionProto() {
    proto::Partition proto = createSimplePartitionProto();
    proto.mutable_status()->set_code(proto::EntityStatus::PUBLISHED);
    proto.mutable_partition_config()->set_version(1);
    proto.mutable_data_source()->add_process_configs()->set_detail("xxx");

    auto tableStructure = proto.mutable_table_structure();
    tableStructure->set_table_name("tb1");
    tableStructure->set_database_name("db1");
    tableStructure->set_catalog_name("ct1");
    tableStructure->mutable_table_structure_config()->mutable_shard_info()->set_shard_count(8);
    tableStructure->set_version(11);
    proto.mutable_operation_meta()->set_created_time(123);
    return proto;
}

TEST_F(PartitionTest, testConvert) {
    {
        proto::Partition inputProto;
        Partition entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::Partition inputProto;
        inputProto.set_partition_name("part1");
        Partition entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::Partition inputProto;
        inputProto.set_partition_name("part1");
        inputProto.set_table_name("t1");
        Partition entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::Partition inputProto;
        inputProto.set_partition_name("part1");
        inputProto.set_table_name("t1");
        inputProto.set_database_name("db1");
        Partition entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::Partition inputProto;
        inputProto.set_partition_name("part1");
        inputProto.set_table_name("t1");
        inputProto.set_database_name("db1");
        inputProto.set_catalog_name("ct1");
        Partition entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::Partition inputProto = createSimplePartitionProto();

        Partition entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(proto::EntityStatus(), entity.status()));
        ASSERT_EQ(5, entity.version());
        ASSERT_EQ("part1", entity.id().partitionName);
        ASSERT_EQ("tb1", entity.id().tableName);
        ASSERT_EQ("db1", entity.id().databaseName);
        ASSERT_EQ("ct1", entity.id().catalogName);
        ASSERT_EQ(proto::PartitionType::TABLE_PARTITION, entity.id().partitionType);
        ASSERT_TRUE(ProtoUtil::compareProto(proto::PartitionConfig(), entity.partitionConfig()));
        ASSERT_TRUE(ProtoUtil::compareProto(proto::DataSource(), entity.dataSource()));
        ASSERT_EQ(nullptr, entity.tableStructure());

        proto::Partition outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(inputProto, outputProto));
    }
    {
        proto::Partition inputProto = createSimplePartitionProto();
        auto tableStructure = inputProto.mutable_table_structure();
        tableStructure->set_version(3);
        Partition entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::Partition inputProto = createSimplePartitionProto();
        auto tableStructure = inputProto.mutable_table_structure();
        tableStructure->set_table_name("tb1");
        Partition entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::Partition inputProto = createSimplePartitionProto();
        auto tableStructure = inputProto.mutable_table_structure();
        tableStructure->set_table_name("tb1");
        tableStructure->set_database_name("db1");
        Partition entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::Partition inputProto = createFullPartitionProto();
        Partition entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(inputProto.status(), entity.status()));
        ASSERT_EQ(5, entity.version());
        ASSERT_EQ("tb1", entity.id().tableName);
        ASSERT_EQ("db1", entity.id().databaseName);
        ASSERT_EQ("ct1", entity.id().catalogName);
        ASSERT_EQ(proto::PartitionType::TABLE_PARTITION, entity.id().partitionType);
        ASSERT_TRUE(ProtoUtil::compareProto(inputProto.partition_config(), entity.partitionConfig()));
        ASSERT_TRUE(ProtoUtil::compareProto(inputProto.data_source(), entity.dataSource()));
        proto::TableStructure expectedProto;
        ASSERT_EQ(Status::OK, entity.tableStructure()->toProto(&expectedProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(inputProto.table_structure(), expectedProto));
        {
            proto::Partition outputProto;
            ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
            ASSERT_TRUE(ProtoUtil::compareProto(inputProto, outputProto));
        }
        {
            proto::Partition outputProto;
            ASSERT_EQ(Status::OK, entity.toProto(&outputProto, proto::DetailLevel::SUMMARY).code());
            auto expected = inputProto;
            {
                auto subProto = expected.mutable_table_structure();
                subProto->clear_table_name();
                subProto->clear_database_name();
                subProto->clear_catalog_name();
                subProto->clear_columns();
                subProto->clear_indexes();
                subProto->mutable_table_structure_config()->clear_comment();
                subProto->mutable_table_structure_config()->clear_shard_info();
                subProto->mutable_table_structure_config()->clear_table_type();
                subProto->clear_operation_meta();
            }
            ASSERT_TRUE(ProtoUtil::compareProto(expected, outputProto));
        }
        {
            proto::Partition outputProto;
            ASSERT_EQ(Status::OK, entity.toProto(&outputProto, proto::DetailLevel::MINIMAL).code());
            auto expected = inputProto;
            expected.clear_table_name();
            expected.clear_database_name();
            expected.clear_catalog_name();
            expected.clear_partition_config();
            expected.clear_data_source();
            expected.clear_table_structure();
            expected.clear_operation_meta();
            expected.clear_partition_type();
            ASSERT_TRUE(ProtoUtil::compareProto(expected, outputProto));
        }
    }
}

TEST_F(PartitionTest, testClone) {
    proto::Partition inputProto = createFullPartitionProto();

    Partition entity;
    ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

    auto newEntity = entity.clone();
    ASSERT_NE(nullptr, newEntity);

    proto::Partition outputProto;
    ASSERT_EQ(Status::OK, newEntity->toProto(&outputProto).code());
    ASSERT_TRUE(ProtoUtil::compareProto(inputProto, outputProto));
}

TEST_F(PartitionTest, testCompare) {
    {
        proto::Partition inputProto = createSimplePartitionProto();
        Partition entity1;
        ASSERT_EQ(Status::OK, entity1.fromProto(inputProto).code());
        Partition entity2;
        ASSERT_EQ(Status::OK, entity2.fromProto(inputProto).code());

        DiffResult diffResult;
        ASSERT_EQ(Status::OK, entity2.compare(&entity1, diffResult).code());
        ASSERT_FALSE(diffResult.hasDiff());
    }
    {
        proto::Partition inputProto2 = createSimplePartitionProto();
        inputProto2.set_version(7);
        inputProto2.mutable_data_source()->add_process_configs()->set_detail("yyy");
        Partition entity2;
        ASSERT_EQ(Status::OK, entity2.fromProto(inputProto2).code());

        DiffResult diffResult;
        ASSERT_EQ(Status::OK, entity2.compare(nullptr, diffResult).code());
        ASSERT_TRUE(diffResult.hasDiff());
        ASSERT_EQ(1UL, diffResult.partitions.size());
        ASSERT_EQ(0UL, diffResult.tableStructures.size());

        proto::PartitionRecord expectedProto;
        expectedProto.set_partition_name("part1");
        expectedProto.set_table_name("tb1");
        expectedProto.set_database_name("db1");
        expectedProto.set_catalog_name("ct1");
        expectedProto.set_partition_type(proto::PartitionType::TABLE_PARTITION);
        expectedProto.set_version(7);
        expectedProto.mutable_data_source()->add_process_configs()->set_detail("yyy");
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, *diffResult.partitions[0]));
    }
    {
        proto::Partition inputProto1 = createSimplePartitionProto();
        Partition entity1;
        ASSERT_EQ(Status::OK, entity1.fromProto(inputProto1).code());

        proto::Partition inputProto2 = createSimplePartitionProto();
        inputProto2.set_version(7);
        inputProto2.mutable_data_source()->add_process_configs()->set_detail("yyy");
        Partition entity2;
        ASSERT_EQ(Status::OK, entity2.fromProto(inputProto2).code());

        DiffResult diffResult;
        ASSERT_EQ(Status::OK, entity2.compare(&entity1, diffResult).code());
        ASSERT_TRUE(diffResult.hasDiff());
        ASSERT_EQ(1UL, diffResult.partitions.size());
        ASSERT_EQ(0UL, diffResult.tableStructures.size());

        proto::PartitionRecord expectedProto;
        expectedProto.set_partition_name("part1");
        expectedProto.set_table_name("tb1");
        expectedProto.set_database_name("db1");
        expectedProto.set_catalog_name("ct1");
        expectedProto.set_partition_type(proto::PartitionType::TABLE_PARTITION);
        expectedProto.set_version(7);
        expectedProto.mutable_data_source()->add_process_configs()->set_detail("yyy");
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, *diffResult.partitions[0]));
    }
    {
        proto::Partition inputProto1 = createSimplePartitionProto();
        Partition entity1;
        ASSERT_EQ(Status::OK, entity1.fromProto(inputProto1).code());

        proto::Partition inputProto2 = createFullPartitionProto();
        inputProto2.set_version(7);
        inputProto2.mutable_data_source()->mutable_process_configs(0)->set_detail("zzz");
        Partition entity2;
        ASSERT_EQ(Status::OK, entity2.fromProto(inputProto2).code());

        DiffResult diffResult;
        ASSERT_EQ(Status::OK, entity2.compare(&entity1, diffResult).code());
        ASSERT_TRUE(diffResult.hasDiff());
        ASSERT_EQ(1UL, diffResult.partitions.size());
        ASSERT_EQ(0UL, diffResult.tableStructures.size());

        proto::PartitionRecord expectedProto;
        expectedProto.set_partition_name("part1");
        expectedProto.set_table_name("tb1");
        expectedProto.set_database_name("db1");
        expectedProto.set_catalog_name("ct1");
        expectedProto.set_partition_type(proto::PartitionType::TABLE_PARTITION);
        expectedProto.set_version(7);
        expectedProto.mutable_data_source()->add_process_configs()->set_detail("zzz");
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PUBLISHED);
        expectedProto.mutable_partition_config()->set_version(1);
        expectedProto.mutable_operation_meta()->set_created_time(123);
        {
            auto subProto = expectedProto.mutable_table_structure_record();
            subProto->set_entity_name("tb1");
            subProto->set_version(11);
        }
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, *diffResult.partitions[0], &diff)) << diff;
    }
}

TEST_F(PartitionTest, testAlignVersion) {
    {
        proto::Partition inputProto = createSimplePartitionProto();
        Partition entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_EQ(5, entity.version());
        entity.alignVersion(20);
        ASSERT_EQ(5, entity.version());
    }
    {
        proto::Partition inputProto = createFullPartitionProto();
        inputProto.set_version(kToUpdateCatalogVersion);
        inputProto.mutable_table_structure()->set_version(kToUpdateCatalogVersion);
        Partition entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_EQ(kToUpdateCatalogVersion, entity.version());
        ASSERT_EQ(kToUpdateCatalogVersion, entity.tableStructure()->version());
        entity.alignVersion(20);
        ASSERT_EQ(20, entity.version());
        ASSERT_EQ(20, entity.tableStructure()->version());
    }
}

TEST_F(PartitionTest, testCreate) {
    { // failure
        proto::Partition inputProto;
        Partition entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.create(inputProto).code());
    }
    { // success
        proto::Partition inputProto = createFullPartitionProto();
        Partition entity;
        ASSERT_EQ(Status::OK, entity.create(inputProto).code());
        auto expectedProto = inputProto;
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        expectedProto.set_version(kToUpdateCatalogVersion);
        proto::Partition outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(PartitionTest, testUpdate) {
    { // data_source update in partition is supported now
        Partition entity;
        proto::Partition inputProto = createFullPartitionProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::Partition request = createFullPartitionProto();
        request.clear_table_structure();
        request.mutable_data_source()->add_process_configs()->set_detail("yyy");
        ASSERT_EQ(Status::OK, entity.update(request).code());
    }
    { // direct update table_structure is supported now
        Partition entity;
        proto::Partition inputProto = createFullPartitionProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::Partition request = createFullPartitionProto();
        request.mutable_table_structure()->mutable_table_structure_config()->mutable_shard_info()->set_shard_count(16);
        ASSERT_EQ(Status::OK, entity.update(request).code());
    }
    { // success
        Partition entity;
        proto::Partition inputProto = createFullPartitionProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::Partition expectedProto = createFullPartitionProto();
        expectedProto.mutable_partition_config()->set_version(11);
        auto request = expectedProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);

        request.clear_data_source();
        request.clear_table_structure();
        ASSERT_EQ(Status::OK, entity.update(request).code());
        ASSERT_EQ(proto::EntityStatus::PENDING_PUBLISH, entity.status().code());
        ASSERT_EQ(kToUpdateCatalogVersion, entity.version());
        proto::Partition outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
    { // success
        Partition entity;
        proto::Partition inputProto = createFullPartitionProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::Partition expectedProto = createFullPartitionProto();
        expectedProto.mutable_partition_config()->set_version(11);
        auto request = expectedProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);

        ASSERT_EQ(Status::OK, entity.update(request).code());
        ASSERT_EQ(proto::EntityStatus::PENDING_PUBLISH, entity.status().code());
        ASSERT_EQ(kToUpdateCatalogVersion, entity.version());
        proto::Partition outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
    { // update partition config
        Partition entity;
        proto::Partition inputProto = createFullPartitionProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::Partition request = createFullPartitionProto();
        request.clear_data_source();
        request.clear_table_structure();
        ASSERT_EQ(Status::OK, entity.update(request).code());
    }
}

TEST_F(PartitionTest, testDrop) {
    Partition entity;
    proto::Partition inputProto = createFullPartitionProto();
    ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
    ASSERT_EQ(Status::OK, entity.drop().code());
    ASSERT_EQ(proto::EntityStatus::PENDING_DELETE, entity.status().code());
    ASSERT_EQ(kToUpdateCatalogVersion, entity.version());
}

TEST_F(PartitionTest, testUpdateTableStructure) {
    Partition entity;
    proto::Partition inputProto = createFullPartitionProto();
    ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
    auto request = inputProto.table_structure();
    request.mutable_table_structure_config()->mutable_shard_info()->set_shard_count(32);
    ASSERT_EQ(Status::OK, entity.updateTableStructure(request).code());
    ASSERT_EQ(proto::EntityStatus::PENDING_PUBLISH, entity.status().code());
    ASSERT_EQ(kToUpdateCatalogVersion, entity.version());
    proto::TableStructure outputProto;
    ASSERT_EQ(Status::OK, entity.tableStructure()->toProto(&outputProto).code());
    ASSERT_TRUE(ProtoUtil::compareProto(request, outputProto));
}

} // namespace catalog
