#include "catalog/entity/Table.h"

#include "gtest/gtest.h"
#include <stdio.h>
#include <string>
#include <vector>

#include "catalog/util/ProtoUtil.h"

namespace catalog {

class TableTest : public ::testing::Test {};

auto createSimpleTableProto() {
    proto::Table proto;
    proto.set_table_name("tb1");
    proto.set_database_name("db1");
    proto.set_catalog_name("ct1");
    {
        auto tableStructure = proto.mutable_table_structure();
        tableStructure->set_table_name("tb1");
        tableStructure->set_database_name("db1");
        tableStructure->set_catalog_name("ct1");
        tableStructure->mutable_table_structure_config()->mutable_shard_info()->set_shard_count(8);
        tableStructure->set_version(7);
    }
    proto.mutable_operation_meta()->set_created_time(123);
    return proto;
}

auto createFullTableProto() {
    proto::Table proto = createSimpleTableProto();
    proto.mutable_status()->set_code(proto::EntityStatus::PUBLISHED);
    proto.set_version(3);
    proto.mutable_table_config()->set_version(1);
    {
        auto partition = proto.add_partitions();
        partition->set_partition_name("part1");
        partition->set_table_name("tb1");
        partition->set_database_name("db1");
        partition->set_catalog_name("ct1");
        partition->set_partition_type(proto::PartitionType::TABLE_PARTITION);
        partition->mutable_partition_config()->set_version(1);
        partition->set_version(2);
    }
    return proto;
}

TEST_F(TableTest, testConvert) {
    {
        proto::Table inputProto;
        Table entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::Table inputProto;
        inputProto.set_table_name("t1");
        Table entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::Table inputProto;
        inputProto.set_table_name("t1");
        inputProto.set_database_name("db1");
        Table entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::Table inputProto;
        inputProto.set_table_name("t1");
        inputProto.set_database_name("db1");
        inputProto.set_catalog_name("ct1");
        Table entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::Table inputProto;
        inputProto.set_table_name("t1");
        inputProto.set_database_name("db1");
        inputProto.set_catalog_name("ct1");
        {
            auto subProto = inputProto.mutable_table_structure();
            subProto->set_version(1);
        }
        Table entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::Table inputProto;
        inputProto.set_table_name("t1");
        inputProto.set_database_name("db1");
        inputProto.set_catalog_name("ct1");
        {
            auto subProto = inputProto.mutable_table_structure();
            subProto->set_version(1);
        }
        Table entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::Table inputProto;
        inputProto.set_table_name("t1");
        inputProto.set_database_name("db1");
        inputProto.set_catalog_name("ct1");
        {
            auto subProto = inputProto.mutable_table_structure();
            subProto->set_table_name("t2");
            subProto->set_version(1);
        }
        Table entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::Table inputProto;
        inputProto.set_table_name("t1");
        inputProto.set_database_name("db1");
        inputProto.set_catalog_name("ct1");
        {
            auto subProto = inputProto.mutable_table_structure();
            subProto->set_table_name("t1");
            subProto->set_database_name("db2");
            subProto->set_version(1);
        }
        Table entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::Table inputProto;
        inputProto.set_table_name("t1");
        inputProto.set_database_name("db1");
        inputProto.set_catalog_name("ct1");
        {
            auto subProto = inputProto.mutable_table_structure();
            subProto->set_table_name("t1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct2");
            subProto->set_version(1);
        }
        Table entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::Table inputProto;
        inputProto.set_table_name("t1");
        inputProto.set_database_name("db1");
        inputProto.set_catalog_name("ct1");
        {
            auto subProto = inputProto.mutable_table_structure();
            subProto->set_table_name("t1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
            subProto->set_version(1);
        }
        {
            auto subProto = inputProto.add_partitions();
            subProto->set_version(1);
        }
        Table entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::Table inputProto = createSimpleTableProto();
        {
            auto subProto = inputProto.add_partitions();
            subProto->set_version(1);
            subProto->set_table_name("t2");
        }
        Table entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::Table inputProto = createSimpleTableProto();
        {
            auto subProto = inputProto.add_partitions();
            subProto->set_version(1);
            subProto->set_table_name("t1");
            subProto->set_database_name("db2");
        }
        Table entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::Table inputProto = createSimpleTableProto();
        {
            auto subProto = inputProto.add_partitions();
            subProto->set_version(1);
            subProto->set_table_name("t1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct2");
        }
        Table entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        auto inputProto = createSimpleTableProto();
        Table entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(proto::EntityStatus(), entity.status()));
        ASSERT_EQ(0, entity.version());
        ASSERT_EQ("tb1", entity.id().tableName);
        ASSERT_EQ("db1", entity.id().databaseName);
        ASSERT_EQ("ct1", entity.id().catalogName);
        ASSERT_TRUE(ProtoUtil::compareProto(proto::TableConfig(), entity.tableConfig()));
        {
            ASSERT_NE(nullptr, entity.tableStructure());
            proto::TableStructure expectedProto;
            ASSERT_EQ(Status::OK, entity.tableStructure()->toProto(&expectedProto).code());
            ASSERT_TRUE(ProtoUtil::compareProto(inputProto.table_structure(), expectedProto));
        }
        ASSERT_EQ(0UL, entity.partitions().size());

        proto::Table outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(inputProto, outputProto));
    }
    {
        auto inputProto = createFullTableProto();
        Table entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(inputProto.status(), entity.status()));
        ASSERT_EQ(3, entity.version());
        ASSERT_EQ("tb1", entity.id().tableName);
        ASSERT_EQ("db1", entity.id().databaseName);
        ASSERT_EQ("ct1", entity.id().catalogName);
        ASSERT_TRUE(ProtoUtil::compareProto(inputProto.table_config(), entity.tableConfig()));
        {
            ASSERT_NE(nullptr, entity.tableStructure());
            proto::TableStructure expectedProto;
            ASSERT_EQ(Status::OK, entity.tableStructure()->toProto(&expectedProto).code());
            ASSERT_TRUE(ProtoUtil::compareProto(inputProto.table_structure(), expectedProto));
        }
        {
            ASSERT_EQ(1UL, entity.partitions().size());
            const auto &partition = entity.partitions().at("part1");
            proto::Partition expectedProto;
            ASSERT_EQ(Status::OK, partition->toProto(&expectedProto).code());
            ASSERT_TRUE(ProtoUtil::compareProto(inputProto.partitions()[0], expectedProto));
        }
        {
            proto::Table outputProto;
            ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
            ASSERT_TRUE(ProtoUtil::compareProto(inputProto, outputProto));
        }
        {
            proto::Table outputProto;
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
            {
                auto subProto = expected.mutable_partitions(0);
                subProto->clear_table_name();
                subProto->clear_database_name();
                subProto->clear_catalog_name();
                subProto->clear_partition_type();
                subProto->clear_partition_config();
                subProto->clear_data_source();
                subProto->clear_table_structure();
                subProto->clear_operation_meta();
            }
            ASSERT_TRUE(ProtoUtil::compareProto(expected, outputProto));
        }
        {
            proto::Table outputProto;
            ASSERT_EQ(Status::OK, entity.toProto(&outputProto, proto::DetailLevel::MINIMAL).code());
            auto expected = inputProto;
            expected.clear_database_name();
            expected.clear_catalog_name();
            expected.clear_table_config();
            expected.clear_partitions();
            expected.clear_table_structure();
            expected.clear_operation_meta();
            std::string diff;
            ASSERT_TRUE(ProtoUtil::compareProto(expected, outputProto, &diff)) << diff;
        }
    }
    { //  partitions duplicated
        auto inputProto = createSimpleTableProto();
        {
            auto subProto = inputProto.add_partitions();
            subProto->set_partition_name("part1");
            subProto->set_table_name("tb1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
            subProto->set_partition_type(proto::PartitionType::TABLE_PARTITION);
        }
        {
            auto subProto = inputProto.add_partitions();
            subProto->set_partition_name("part1");
            subProto->set_table_name("tb1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
            subProto->set_partition_type(proto::PartitionType::TABLE_PARTITION);
        }
        Table entity;
        ASSERT_EQ(Status::DUPLICATE_ENTITY, entity.fromProto(inputProto).code());
    }
    { // check toProto: partitions sorted by name
        auto inputProto = createSimpleTableProto();
        {
            auto subProto = inputProto.add_partitions();
            subProto->set_partition_name("part2");
            subProto->set_table_name("tb1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
            subProto->set_partition_type(proto::PartitionType::TABLE_PARTITION);
            subProto->mutable_partition_config()->set_version(1);
        }
        {
            auto subProto = inputProto.add_partitions();
            subProto->set_partition_name("part1");
            subProto->set_table_name("tb1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
            subProto->set_partition_type(proto::PartitionType::TABLE_PARTITION);
            subProto->mutable_partition_config()->set_version(1);
        }
        Table entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::Table outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        auto expectedProto = createSimpleTableProto();
        expectedProto.add_partitions()->CopyFrom(inputProto.partitions(1));
        expectedProto.add_partitions()->CopyFrom(inputProto.partitions(0));
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(TableTest, testClone) {
    auto inputProto = createFullTableProto();

    Table entity;
    ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

    auto newEntity = entity.clone();
    ASSERT_NE(nullptr, newEntity);

    proto::Table outputProto;
    ASSERT_EQ(Status::OK, newEntity->toProto(&outputProto).code());
    ASSERT_TRUE(ProtoUtil::compareProto(inputProto, outputProto));
}

TEST_F(TableTest, testCompare) {
    {
        auto inputProto1 = createFullTableProto();
        Table entity1;
        ASSERT_EQ(Status::OK, entity1.fromProto(inputProto1).code());

        auto inputProto2 = createFullTableProto();
        Table entity2;
        ASSERT_EQ(Status::OK, entity2.fromProto(inputProto2).code());
        {
            DiffResult diffResult;
            ASSERT_EQ(Status::OK, entity1.compare(&entity2, diffResult).code());
            ASSERT_FALSE(diffResult.hasDiff());
        }
        {
            DiffResult diffResult;
            ASSERT_EQ(Status::OK, entity2.compare(&entity1, diffResult).code());
            ASSERT_FALSE(diffResult.hasDiff());
        }
    }
    {
        auto inputProto2 = createFullTableProto();
        inputProto2.set_version(10);
        Table entity2;
        ASSERT_EQ(Status::OK, entity2.fromProto(inputProto2).code());

        DiffResult diffResult;
        ASSERT_EQ(Status::OK, entity2.compare(nullptr, diffResult).code());
        ASSERT_TRUE(diffResult.hasDiff());
        ASSERT_EQ(1UL, diffResult.tables.size());

        proto::TableRecord expectedProto;
        expectedProto.set_table_name("tb1");
        expectedProto.set_database_name("db1");
        expectedProto.set_catalog_name("ct1");
        expectedProto.set_version(10);
        {
            auto subProto = expectedProto.mutable_table_config();
            subProto->set_version(1);
        }
        {
            auto subProto = expectedProto.mutable_table_structure_record();
            subProto->set_entity_name("tb1");
            subProto->set_version(7);
        }
        {
            auto subProto = expectedProto.add_partition_records();
            subProto->set_entity_name("part1");
            subProto->set_version(2);
        }
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PUBLISHED);
        expectedProto.mutable_operation_meta()->set_created_time(123);
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, *diffResult.tables[0], &diff)) << diff;

        ASSERT_EQ(1UL, diffResult.tableStructures.size());
        ASSERT_EQ(7, diffResult.tableStructures[0]->version());
        ASSERT_EQ(1UL, diffResult.partitions.size());
        ASSERT_EQ("part1", diffResult.partitions[0]->partition_name());
        ASSERT_EQ(2, diffResult.partitions[0]->version());
    }
    {
        auto inputProto1 = createFullTableProto();
        Table entity1;
        ASSERT_EQ(Status::OK, entity1.fromProto(inputProto1).code());

        auto inputProto2 = createFullTableProto();
        inputProto2.set_version(10);
        Table entity2;
        ASSERT_EQ(Status::OK, entity2.fromProto(inputProto2).code());

        DiffResult diffResult;
        ASSERT_EQ(Status::OK, entity2.compare(&entity1, diffResult).code());
        ASSERT_TRUE(diffResult.hasDiff());
        ASSERT_EQ(1UL, diffResult.tables.size());
        ASSERT_EQ(0UL, diffResult.tableStructures.size());
        ASSERT_EQ(0UL, diffResult.partitions.size());

        proto::TableRecord expectedProto;
        expectedProto.set_table_name("tb1");
        expectedProto.set_database_name("db1");
        expectedProto.set_catalog_name("ct1");
        expectedProto.set_version(10);
        {
            auto subProto = expectedProto.mutable_table_config();
            subProto->set_version(1);
        }
        {
            auto subProto = expectedProto.mutable_table_structure_record();
            subProto->set_entity_name("tb1");
            subProto->set_version(7);
        }
        {
            auto subProto = expectedProto.add_partition_records();
            subProto->set_entity_name("part1");
            subProto->set_version(2);
        }
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PUBLISHED);
        expectedProto.mutable_operation_meta()->set_created_time(123);
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, *diffResult.tables[0], &diff)) << diff;
    }
    {
        auto inputProto1 = createFullTableProto();
        Table entity1;
        ASSERT_EQ(Status::OK, entity1.fromProto(inputProto1).code());

        auto inputProto2 = createFullTableProto();
        inputProto2.set_version(10);
        {
            auto tableStructure = inputProto2.mutable_table_structure();
            tableStructure->set_table_name("tb1");
            tableStructure->set_database_name("db1");
            tableStructure->set_catalog_name("ct1");
            tableStructure->set_version(8);
        }
        {
            auto partition = inputProto2.add_partitions();
            partition->set_partition_name("part2");
            partition->set_table_name("tb1");
            partition->set_database_name("db1");
            partition->set_catalog_name("ct1");
            partition->set_partition_type(proto::PartitionType::TABLE_PARTITION);
            partition->set_version(3);
        }
        Table entity2;
        ASSERT_EQ(Status::OK, entity2.fromProto(inputProto2).code());

        DiffResult diffResult;
        ASSERT_EQ(Status::OK, entity2.compare(&entity1, diffResult).code());
        ASSERT_TRUE(diffResult.hasDiff());
        ASSERT_EQ(1UL, diffResult.tables.size());
        {
            proto::TableRecord expectedProto;
            expectedProto.set_table_name("tb1");
            expectedProto.set_database_name("db1");
            expectedProto.set_catalog_name("ct1");
            expectedProto.set_version(10);
            {
                auto subProto = expectedProto.mutable_table_config();
                subProto->set_version(1);
            }
            {
                auto subProto = expectedProto.mutable_table_structure_record();
                subProto->set_entity_name("tb1");
                subProto->set_version(8);
            }
            {
                auto subProto = expectedProto.add_partition_records();
                subProto->set_entity_name("part1");
                subProto->set_version(2);
            }
            {
                auto subProto = expectedProto.add_partition_records();
                subProto->set_entity_name("part2");
                subProto->set_version(3);
            }
            expectedProto.mutable_status()->set_code(proto::EntityStatus::PUBLISHED);
            expectedProto.mutable_operation_meta()->set_created_time(123);
            std::string diff;
            ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, *diffResult.tables[0], &diff)) << diff;
        }
        ASSERT_EQ(1UL, diffResult.tableStructures.size());
        ASSERT_EQ(8, diffResult.tableStructures[0]->version());
        ASSERT_EQ(1UL, diffResult.partitions.size());
        ASSERT_EQ("part2", diffResult.partitions[0]->partition_name());
        ASSERT_EQ(3, diffResult.partitions[0]->version());
    }
}

TEST_F(TableTest, testAlignVersion) {
    {
        proto::Table inputProto = createFullTableProto();
        Table entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_EQ(3, entity.version());
        ASSERT_EQ(7, entity.tableStructure()->version());
        ASSERT_EQ(2, entity.partitions().at("part1")->version());
        entity.alignVersion(20);
        ASSERT_EQ(3, entity.version());
        ASSERT_EQ(7, entity.tableStructure()->version());
        ASSERT_EQ(2, entity.partitions().at("part1")->version());
    }
    {
        proto::Table inputProto = createFullTableProto();
        inputProto.set_version(kToUpdateCatalogVersion);
        inputProto.mutable_table_structure()->set_version(kToUpdateCatalogVersion);
        inputProto.mutable_partitions(0)->set_version(kToUpdateCatalogVersion);
        Table entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_EQ(kToUpdateCatalogVersion, entity.version());
        ASSERT_EQ(kToUpdateCatalogVersion, entity.tableStructure()->version());
        ASSERT_EQ(kToUpdateCatalogVersion, entity.partitions().at("part1")->version());
        entity.alignVersion(20);
        ASSERT_EQ(20, entity.version());
        ASSERT_EQ(20, entity.tableStructure()->version());
        ASSERT_EQ(20, entity.partitions().at("part1")->version());
    }
}

TEST_F(TableTest, testCreate) {
    { // simple case: no partition
        proto::Table inputProto = createSimpleTableProto();
        Table entity;
        ASSERT_EQ(Status::OK, entity.create(inputProto).code());

        auto expectedProto = inputProto;
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        expectedProto.set_version(kToUpdateCatalogVersion);
        {
            auto subProto = expectedProto.mutable_table_structure();
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            subProto->set_version(kToUpdateCatalogVersion);
        }
        proto::Table outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
    { // complex case: with partition
        proto::Table inputProto = createFullTableProto();
        Table entity;
        ASSERT_EQ(Status::OK, entity.create(inputProto).code());

        auto expectedProto = inputProto;
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        expectedProto.set_version(kToUpdateCatalogVersion);
        {
            auto subProto = expectedProto.mutable_table_structure();
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            subProto->set_version(kToUpdateCatalogVersion);
        }
        {
            auto subProto = expectedProto.mutable_partitions(0);
            subProto->mutable_table_structure()->CopyFrom(expectedProto.table_structure());
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            subProto->set_version(kToUpdateCatalogVersion);
        }
        proto::Table outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
    { // no table_structure
        proto::Table inputProto = createFullTableProto();
        inputProto.clear_table_structure();
        Table entity;
        ASSERT_EQ(Status::UNSUPPORTED, entity.create(inputProto).code());
    }
    { // partition has different table_structure
        proto::Table inputProto = createFullTableProto();
        inputProto.add_partitions()->mutable_table_structure()->mutable_table_structure_config()->set_comment("xxx");
        Table entity;
        ASSERT_EQ(Status::UNSUPPORTED, entity.create(inputProto).code());
    }
}

TEST_F(TableTest, testDrop) {
    auto inputProto = createFullTableProto();
    Table entity;
    ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
    ASSERT_NE(proto::EntityStatus::PENDING_DELETE, entity.status().code());
    ASSERT_NE(kToUpdateCatalogVersion, entity.version());
    ASSERT_EQ(Status::OK, entity.drop().code());
    ASSERT_EQ(proto::EntityStatus::PENDING_DELETE, entity.status().code());
    ASSERT_EQ(kToUpdateCatalogVersion, entity.version());
}

TEST_F(TableTest, testUpdate) {
    { // direct update table_structure is unsupported
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::Table request = createFullTableProto();
        request.mutable_table_structure()->mutable_table_structure_config()->mutable_shard_info()->set_shard_count(16);
        ASSERT_EQ(Status::UNSUPPORTED, entity.update(request).code());
    }
    { // success
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::Table expectedProto = createFullTableProto();
        expectedProto.mutable_table_config()->set_version(11);
        auto request = expectedProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);

        request.clear_table_structure();
        ASSERT_EQ(Status::OK, entity.update(request).code());
        ASSERT_EQ(proto::EntityStatus::PENDING_PUBLISH, entity.status().code());
        ASSERT_EQ(kToUpdateCatalogVersion, entity.version());
        proto::Table outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
    { // success
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::Table expectedProto = createFullTableProto();
        expectedProto.mutable_table_config()->set_version(11);
        auto request = expectedProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);

        ASSERT_EQ(Status::OK, entity.update(request).code());
        ASSERT_EQ(proto::EntityStatus::PENDING_PUBLISH, entity.status().code());
        ASSERT_EQ(kToUpdateCatalogVersion, entity.version());
        proto::Table outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
    { // no acutal update
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::Table request = createFullTableProto();
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.update(request).code());
    }
}

TEST_F(TableTest, testUpdateTableStructure) {
    { // failure
        Table entity;
        proto::Table inputProto = createFullTableProto();
        auto request = inputProto.table_structure();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.updateTableStructure(request).code());
    }
    { // success
        Table entity;
        proto::Table inputProto = createFullTableProto();
        auto request = inputProto.table_structure();
        request.mutable_table_structure_config()->mutable_shard_info()->set_shard_count(16);
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_EQ(Status::OK, entity.updateTableStructure(request).code());
        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_table_structure();
            subProto->CopyFrom(request);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Table outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(TableTest, testAddColumn) {
    { // failure: no columns
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::AddColumnRequest request;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.addColumn(request).code());
    }
    { // failure: columns duplicated
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::AddColumnRequest request;
        request.add_columns()->set_name("f1");
        request.add_columns()->set_name("f1");
        ASSERT_NE(Status::OK, entity.addColumn(request).code());
    }
    { // success
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::AddColumnRequest request;
        request.add_columns()->set_name("f1");
        request.add_columns()->set_name("f2");
        ASSERT_EQ(Status::OK, entity.addColumn(request).code());

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_table_structure();
            subProto->add_columns()->CopyFrom(request.columns(0));
            subProto->add_columns()->CopyFrom(request.columns(1));
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Table outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(TableTest, testUpdateColumn) {
    { // failure: no column
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::UpdateColumnRequest request;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.updateColumn(request).code());
    }
    { // failure: column not found
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::UpdateColumnRequest request;
        request.mutable_column()->set_name("f1");
        ASSERT_NE(Status::OK, entity.updateColumn(request).code());
    }
    { // success
        Table entity;
        proto::Table inputProto = createFullTableProto();
        inputProto.mutable_table_structure()->add_columns()->set_name("f1");
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::UpdateColumnRequest request;
        auto column = request.mutable_column();
        column->set_name("f1");
        column->set_comment("xxx");
        ASSERT_EQ(Status::OK, entity.updateColumn(request).code());

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_table_structure();
            subProto->mutable_columns(0)->CopyFrom(request.column());
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Table outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(TableTest, testDropColumn) {
    { // failure: no column_names
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::DropColumnRequest request;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.dropColumn(request).code());
    }
    { // failure: column_name not found
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::DropColumnRequest request;
        request.add_column_names("f1");
        ASSERT_NE(Status::OK, entity.dropColumn(request).code());
    }
    { // success
        Table entity;
        proto::Table inputProto = createFullTableProto();
        inputProto.mutable_table_structure()->add_columns()->set_name("f1");
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::DropColumnRequest request;
        request.add_column_names("f1");
        ASSERT_EQ(Status::OK, entity.dropColumn(request).code());

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_table_structure();
            subProto->clear_columns();
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Table outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(TableTest, testCreateIndex) {
    { // failure: no indexes
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::CreateIndexRequest request;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.createIndex(request).code());
    }
    { // failure: indexes duplicated
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::CreateIndexRequest request;
        request.add_indexes()->set_name("f1");
        request.add_indexes()->set_name("f1");
        ASSERT_NE(Status::OK, entity.createIndex(request).code());
    }
    { // success
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::CreateIndexRequest request;
        request.add_indexes()->set_name("f1");
        request.add_indexes()->set_name("f2");
        ASSERT_EQ(Status::OK, entity.createIndex(request).code());

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_table_structure();
            subProto->add_indexes()->CopyFrom(request.indexes(0));
            subProto->add_indexes()->CopyFrom(request.indexes(1));
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Table outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(TableTest, testUpdateIndex) {
    { // failure: no column
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::UpdateIndexRequest request;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.updateIndex(request).code());
    }
    { // failure: column not found
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::UpdateIndexRequest request;
        request.mutable_index()->set_name("f1");
        ASSERT_NE(Status::OK, entity.updateIndex(request).code());
    }
    { // success
        Table entity;
        proto::Table inputProto = createFullTableProto();
        inputProto.mutable_table_structure()->add_indexes()->set_name("f1");
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::UpdateIndexRequest request;
        auto column = request.mutable_index();
        column->set_name("f1");
        column->set_index_type(proto::TableStructure::Index::NUMBER);
        ASSERT_EQ(Status::OK, entity.updateIndex(request).code());

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_table_structure();
            subProto->mutable_indexes(0)->CopyFrom(request.index());
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Table outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(TableTest, testDropIndex) {
    { // failure: no column_names
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::DropIndexRequest request;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.dropIndex(request).code());
    }
    { // failure: column_name not found
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::DropIndexRequest request;
        request.add_index_names("f1");
        ASSERT_NE(Status::OK, entity.dropIndex(request).code());
    }
    { // success
        Table entity;
        proto::Table inputProto = createFullTableProto();
        inputProto.mutable_table_structure()->add_indexes()->set_name("f1");
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::DropIndexRequest request;
        request.add_index_names("f1");
        ASSERT_EQ(Status::OK, entity.dropIndex(request).code());

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_table_structure();
            subProto->clear_indexes();
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Table outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(TableTest, testCreatePartition) {
    { // failure: invalid request
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::Partition request;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.createPartition(request).code());
    }
    { // failure: invalid request
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::Partition request;
        request.set_table_name("tb2");
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.createPartition(request).code());
    }
    { // failure: invalid request
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::Partition request;
        request.set_table_name("tb1");
        request.set_database_name("db2");
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.createPartition(request).code());
    }
    { // failure: invalid request
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::Partition request;
        request.set_table_name("tb1");
        request.set_database_name("db1");
        request.set_catalog_name("ct2");
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.createPartition(request).code());
    }
    { // failure: invalid request
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::Partition request;
        request.set_table_name("tb1");
        request.set_database_name("db1");
        request.set_catalog_name("ct1");
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.createPartition(request).code());
    }
    { // failure: partition already exists
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        auto request = inputProto.partitions(0);
        ASSERT_EQ(Status::DUPLICATE_ENTITY, entity.createPartition(request).code());
    }
    { // failure: partition with custom table_structure
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        auto request = inputProto.partitions(0);
        request.set_partition_name("part2");
        request.mutable_table_structure()->set_version(1);
        ASSERT_EQ(Status::UNSUPPORTED, entity.createPartition(request).code());
    }
    { // success
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        auto request = inputProto.partitions(0);
        request.set_partition_name("part2");
        request.clear_table_structure();
        ASSERT_EQ(Status::OK, entity.createPartition(request).code());

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.add_partitions();
            subProto->CopyFrom(request);
            subProto->mutable_table_structure()->CopyFrom(inputProto.table_structure());
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Table outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

TEST_F(TableTest, testDropPartition) {
    { // failure: partition not found
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        PartitionId request{"unknown", "tb1", "db1", "ct1", proto::PartitionType::TABLE_PARTITION};
        ASSERT_EQ(Status::NOT_FOUND, entity.dropPartition(request).code());
    }
    { // success
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        PartitionId request{"part1", "tb1", "db1", "ct1", proto::PartitionType::TABLE_PARTITION};
        ASSERT_EQ(Status::OK, entity.dropPartition(request).code());

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        expectedProto.clear_partitions();
        proto::Table outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(TableTest, testUpdatePartition) {
    { // failure: partition not found
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::Partition request;
        request.set_partition_name("unknown");
        ASSERT_EQ(Status::NOT_FOUND, entity.updatePartition(request).code());
    }
    { // failure: update failure
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::Partition request = inputProto.partitions(0);
        ASSERT_NE(Status::OK, entity.updatePartition(request).code());
    }
    { // success
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::Partition request = inputProto.partitions(0);
        request.mutable_partition_config()->set_version(11);
        ASSERT_EQ(Status::OK, entity.updatePartition(request).code());

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_partitions(0);
            subProto->CopyFrom(request);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Table outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(TableTest, testUpdatePartitionTableStructure) {
    { // failure: partition not found
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::UpdatePartitionTableStructureRequest request;
        request.set_partition_name("unknown");
        ASSERT_EQ(Status::NOT_FOUND, entity.updatePartitionTableStructure(request).code());
    }
    { // success
        Table entity;
        proto::Table inputProto = createFullTableProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::UpdatePartitionTableStructureRequest request;
        request.set_partition_name("part1");
        ASSERT_EQ(Status::OK, entity.updatePartitionTableStructure(request).code());

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_partitions(0);
            subProto->mutable_table_structure()->CopyFrom(inputProto.table_structure());
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Table outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(TableTest, getPartition) {
    Table entity;
    proto::Table inputProto = createFullTableProto();
    ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
    Partition *partition = nullptr;
    ASSERT_EQ(Status::NOT_FOUND, entity.getPartition("unknown", partition).code());
    ASSERT_EQ(nullptr, partition);
    ASSERT_EQ(Status::OK, entity.getPartition("part1", partition).code());
    ASSERT_NE(nullptr, partition);
}

TEST_F(TableTest, listPartition) {
    Table entity;
    ASSERT_EQ(std::vector<std::string>{}, entity.listPartition());
    proto::Table inputProto = createFullTableProto();
    ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
    ASSERT_EQ(std::vector<std::string>{"part1"}, entity.listPartition());
}

} // namespace catalog
