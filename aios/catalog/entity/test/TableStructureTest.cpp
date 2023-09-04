#include "catalog/entity/TableStructure.h"

#include "gtest/gtest.h"
#include <string>

#include "catalog/util/ProtoUtil.h"

namespace catalog {

class TableStructureTest : public ::testing::Test {};

TEST_F(TableStructureTest, testConvert) {
    {
        proto::TableStructure inputProto;
        TableStructure entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::TableStructure inputProto;
        inputProto.set_table_name("t1");
        TableStructure entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::TableStructure inputProto;
        inputProto.set_table_name("t1");
        inputProto.set_database_name("db1");
        TableStructure entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::TableStructure inputProto;
        inputProto.set_table_name("tb1");
        inputProto.set_database_name("db1");
        inputProto.set_catalog_name("ct1");

        TableStructure entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(proto::EntityStatus(), entity.status()));
        ASSERT_EQ(0, entity.version());
        ASSERT_EQ("tb1", entity.id().tableName);
        ASSERT_EQ("db1", entity.id().databaseName);
        ASSERT_EQ("ct1", entity.id().catalogName);
        ASSERT_EQ(0UL, entity.columns().size());
        ASSERT_EQ(0UL, entity.indexes().size());
        ASSERT_TRUE(ProtoUtil::compareProto(proto::TableStructureConfig(), entity.tableStructureConfig()));
        ASSERT_TRUE(ProtoUtil::compareProto(proto::TableStructureConfig::ShardInfo(), entity.shardInfo()));
        ASSERT_EQ(proto::TableType::UNKNOWN, entity.tableStructureConfig().table_type());
        ASSERT_TRUE(entity.comment().empty());

        proto::TableStructure outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(inputProto, outputProto));
    }
    {
        proto::TableStructure inputProto;
        inputProto.set_version(3);
        auto status = inputProto.mutable_status();
        status->set_code(proto::EntityStatus::PUBLISHED);
        inputProto.set_table_name("tb1");
        inputProto.set_database_name("db1");
        inputProto.set_catalog_name("ct1");
        auto column1 = inputProto.add_columns();
        column1->set_name("f1");
        column1->set_default_value("abc");
        auto column2 = inputProto.add_columns();
        column2->set_name("f2");
        column2->set_default_value("def");
        auto index1 = inputProto.add_indexes();
        index1->set_name("idx1");
        auto index2 = inputProto.add_indexes();
        index2->set_name("idx2");
        auto tableStructureConfig = inputProto.mutable_table_structure_config();
        tableStructureConfig->set_table_type(proto::TableType::KV);
        auto shardInfo = inputProto.mutable_table_structure_config()->mutable_shard_info();
        shardInfo->set_shard_count(8);
        shardInfo->set_shard_func("default");
        inputProto.mutable_table_structure_config()->set_table_type(proto::TableType::KV);
        inputProto.mutable_table_structure_config()->set_comment("xxx");

        inputProto.mutable_operation_meta()->set_created_time(123);
        TableStructure entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(*status, entity.status()));
        ASSERT_EQ(3, entity.version());
        ASSERT_EQ("tb1", entity.id().tableName);
        ASSERT_EQ("db1", entity.id().databaseName);
        ASSERT_EQ("ct1", entity.id().catalogName);
        ASSERT_EQ(2UL, entity.columns().size());
        ASSERT_TRUE(ProtoUtil::compareProto(*column1, entity.columns()[0]));
        ASSERT_TRUE(ProtoUtil::compareProto(*column2, entity.columns()[1]));
        ASSERT_EQ(2UL, entity.indexes().size());
        ASSERT_TRUE(ProtoUtil::compareProto(*index1, entity.indexes()[0]));
        ASSERT_TRUE(ProtoUtil::compareProto(*index2, entity.indexes()[1]));
        ASSERT_TRUE(ProtoUtil::compareProto(*tableStructureConfig, entity.tableStructureConfig()));
        ASSERT_TRUE(ProtoUtil::compareProto(*shardInfo, entity.tableStructureConfig().shard_info()));
        ASSERT_EQ("xxx", entity.tableStructureConfig().comment());
        ASSERT_EQ(proto::TableType::KV, entity.tableStructureConfig().table_type());
        {
            proto::TableStructure outputProto;
            ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
            ASSERT_TRUE(ProtoUtil::compareProto(inputProto, outputProto));
        }
        {
            proto::TableStructure outputProto;
            ASSERT_EQ(Status::OK, entity.toProto(&outputProto, proto::DetailLevel::SUMMARY).code());
            ASSERT_TRUE(ProtoUtil::compareProto(inputProto, outputProto));
        }
        {
            proto::TableStructure outputProto;
            ASSERT_EQ(Status::OK, entity.toProto(&outputProto, proto::DetailLevel::MINIMAL).code());
            auto expected = inputProto;
            expected.clear_table_name();
            expected.clear_database_name();
            expected.clear_catalog_name();
            expected.clear_columns();
            expected.clear_indexes();
            expected.clear_table_structure_config();
            expected.clear_operation_meta();
            std::string diff;
            ASSERT_TRUE(ProtoUtil::compareProto(expected, outputProto, &diff)) << diff;
        }
    }
}

TEST_F(TableStructureTest, testClone) {
    proto::TableStructure inputProto;
    inputProto.set_version(3);
    auto status = inputProto.mutable_status();
    status->set_code(proto::EntityStatus::PUBLISHED);
    inputProto.set_table_name("tb1");
    inputProto.set_database_name("db1");
    inputProto.set_catalog_name("ct1");
    auto column1 = inputProto.add_columns();
    column1->set_name("f1");
    column1->set_default_value("abc");
    auto column2 = inputProto.add_columns();
    column2->set_name("f2");
    column2->set_default_value("def");
    auto index1 = inputProto.add_indexes();
    index1->set_name("idx1");
    auto index2 = inputProto.add_indexes();
    index2->set_name("idx2");
    auto tableStructureConfig = inputProto.mutable_table_structure_config();
    tableStructureConfig->set_table_type(proto::TableType::KV);
    TableStructure entity;
    ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

    auto newEntity = entity.clone();
    ASSERT_NE(nullptr, newEntity);
    proto::TableStructure outputProto;
    ASSERT_EQ(Status::OK, newEntity->toProto(&outputProto).code());
    ASSERT_TRUE(ProtoUtil::compareProto(inputProto, outputProto));
}

TEST_F(TableStructureTest, testCompare) {
    TableStructure entity1;
    proto::TableStructure inputProto1;
    inputProto1.set_table_name("tb1");
    inputProto1.set_database_name("db1");
    inputProto1.set_catalog_name("ct1");
    inputProto1.set_version(1);
    inputProto1.mutable_table_structure_config()->mutable_shard_info()->set_shard_count(4);
    ASSERT_EQ(Status::OK, entity1.fromProto(inputProto1).code());
    {
        DiffResult diffResult;
        ASSERT_EQ(Status::OK, entity1.compare(&entity1, diffResult).code());
        ASSERT_FALSE(diffResult.hasDiff());
    }

    TableStructure entity2;
    proto::TableStructure inputProto2;
    inputProto2.set_table_name("tb1");
    inputProto2.set_database_name("db1");
    inputProto2.set_catalog_name("ct1");
    inputProto2.set_version(2);
    inputProto2.mutable_table_structure_config()->mutable_shard_info()->set_shard_count(8);
    ASSERT_EQ(Status::OK, entity2.fromProto(inputProto2).code());
    {
        DiffResult diffResult;
        ASSERT_EQ(Status::OK, entity2.compare(&entity1, diffResult).code());
        ASSERT_TRUE(diffResult.hasDiff());
        ASSERT_EQ(1UL, diffResult.tableStructures.size());
        ASSERT_TRUE(ProtoUtil::compareProto(inputProto2, *diffResult.tableStructures[0]));
    }
    {
        DiffResult diffResult;
        ASSERT_EQ(Status::OK, entity2.compare(nullptr, diffResult).code());
        ASSERT_TRUE(diffResult.hasDiff());
        ASSERT_EQ(1UL, diffResult.tableStructures.size());
        ASSERT_TRUE(ProtoUtil::compareProto(inputProto2, *diffResult.tableStructures[0]));
    }
}

TEST_F(TableStructureTest, testAlignVersion) {
    {
        proto::TableStructure inputProto;
        inputProto.set_table_name("tb1");
        inputProto.set_database_name("db1");
        inputProto.set_catalog_name("ct1");

        TableStructure entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_EQ(0, entity.version());
        entity.alignVersion(3);
        ASSERT_EQ(0, entity.version());
    }
    {
        proto::TableStructure inputProto;
        inputProto.set_table_name("tb1");
        inputProto.set_database_name("db1");
        inputProto.set_catalog_name("ct1");
        inputProto.set_version(kToUpdateCatalogVersion);

        TableStructure entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_EQ(kToUpdateCatalogVersion, entity.version());
        entity.alignVersion(3);
        ASSERT_EQ(3, entity.version());
    }
}

TEST_F(TableStructureTest, testCreate) {
    { // success
        proto::TableStructure inputProto;
        TableStructure entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.create(inputProto).code());
    }
    { // failure
        proto::TableStructure inputProto;
        inputProto.set_version(8);
        auto status = inputProto.mutable_status();
        status->set_code(proto::EntityStatus::PUBLISHED);
        inputProto.set_table_name("tb1");
        inputProto.set_database_name("db1");
        inputProto.set_catalog_name("ct1");
        auto column1 = inputProto.add_columns();
        column1->set_name("f1");
        column1->set_default_value("abc");
        auto index1 = inputProto.add_indexes();
        index1->set_name("idx1");
        auto tableStructureConfig = inputProto.mutable_table_structure_config();
        tableStructureConfig->set_table_type(proto::TableType::KV);

        TableStructure entity;
        ASSERT_EQ(Status::OK, entity.create(inputProto).code());

        auto expectedProto = inputProto;
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        expectedProto.set_version(kToUpdateCatalogVersion);
        proto::TableStructure outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(TableStructureTest, testUpdate) {
    TableStructure entity;
    {
        proto::TableStructure inputProto;
        inputProto.set_table_name("tb1");
        inputProto.set_database_name("db1");
        inputProto.set_catalog_name("ct1");
        inputProto.set_version(3);
        auto status = inputProto.mutable_status();
        status->set_code(proto::EntityStatus::PUBLISHED);
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
    }

    proto::TableStructure request;
    request.set_version(8);
    auto status = request.mutable_status();
    status->set_code(proto::EntityStatus::PUBLISHED);
    request.set_table_name("tb1");
    request.set_database_name("db1");
    request.set_catalog_name("ct1");
    auto column1 = request.add_columns();
    column1->set_name("f1");
    column1->set_default_value("abc");
    auto index1 = request.add_indexes();
    index1->set_name("idx1");
    auto tableStructureConfig = request.mutable_table_structure_config();
    tableStructureConfig->set_table_type(proto::TableType::KV);

    ASSERT_EQ(Status::OK, entity.update(request).code());

    ASSERT_EQ(proto::EntityStatus::PENDING_PUBLISH, entity.status().code());
    ASSERT_EQ(kToUpdateCatalogVersion, entity.version());
    ASSERT_EQ("tb1", entity.id().tableName);
    ASSERT_EQ("db1", entity.id().databaseName);
    ASSERT_EQ("ct1", entity.id().catalogName);
    ASSERT_EQ(1UL, entity.columns().size());
    ASSERT_TRUE(ProtoUtil::compareProto(*column1, entity.columns()[0]));
    ASSERT_EQ(1UL, entity.indexes().size());
    ASSERT_TRUE(ProtoUtil::compareProto(*index1, entity.indexes()[0]));
    ASSERT_TRUE(ProtoUtil::compareProto(*tableStructureConfig, entity.tableStructureConfig()));

    // no acutal update
    ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.update(request).code());
}

TEST_F(TableStructureTest, testDrop) {
    TableStructure entity;
    {
        proto::TableStructure inputProto;
        inputProto.set_table_name("tb1");
        inputProto.set_database_name("db1");
        inputProto.set_catalog_name("ct1");
        inputProto.set_version(3);
        auto status = inputProto.mutable_status();
        status->set_code(proto::EntityStatus::PUBLISHED);
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
    }
    ASSERT_EQ(Status::OK, entity.drop().code());
    ASSERT_EQ(proto::EntityStatus::PENDING_DELETE, entity.status().code());
    ASSERT_EQ(kToUpdateCatalogVersion, entity.version());
}

TEST_F(TableStructureTest, testAddColumn) {
    TableStructure entity;
    {
        proto::TableStructure inputProto;
        inputProto.set_table_name("tb1");
        inputProto.set_database_name("db1");
        inputProto.set_catalog_name("ct1");
        inputProto.set_version(3);
        inputProto.add_columns()->set_name("f1");
        inputProto.mutable_status()->set_code(proto::EntityStatus::PUBLISHED);
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_EQ(1UL, entity.columns().size());
    }
    {
        proto::TableStructure::Column column;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.addColumn(column).code());
        ASSERT_EQ(1UL, entity.columns().size());
    }
    proto::TableStructure::Column column;
    column.set_name("f2");
    {
        ASSERT_EQ(Status::OK, entity.addColumn(column).code());
        ASSERT_EQ(2UL, entity.columns().size());
        ASSERT_TRUE(ProtoUtil::compareProto(column, entity.columns()[1]));
        ASSERT_EQ(proto::EntityStatus::PENDING_PUBLISH, entity.status().code());
        ASSERT_EQ(kToUpdateCatalogVersion, entity.version());
    }
    {
        ASSERT_EQ(Status::DUPLICATE_ENTITY, entity.addColumn(column).code());
        ASSERT_EQ(2UL, entity.columns().size());
    }
}

TEST_F(TableStructureTest, testUpdateColumn) {
    TableStructure entity;
    {
        proto::TableStructure inputProto;
        inputProto.set_table_name("tb1");
        inputProto.set_database_name("db1");
        inputProto.set_catalog_name("ct1");
        inputProto.set_version(3);
        inputProto.add_columns()->set_name("f1");
        inputProto.mutable_status()->set_code(proto::EntityStatus::PUBLISHED);
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_EQ(1UL, entity.columns().size());
    }
    {
        proto::TableStructure::Column column;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.updateColumn(column).code());
        ASSERT_EQ(1UL, entity.columns().size());
    }
    proto::TableStructure::Column column;
    column.set_name("f1");
    {
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.updateColumn(column).code());
        ASSERT_EQ(1UL, entity.columns().size());
    }
    column.set_default_value("abc");
    {
        ASSERT_EQ(Status::OK, entity.updateColumn(column).code());
        ASSERT_EQ(1UL, entity.columns().size());
        ASSERT_TRUE(ProtoUtil::compareProto(column, entity.columns()[0]));
        ASSERT_EQ(proto::EntityStatus::PENDING_PUBLISH, entity.status().code());
        ASSERT_EQ(kToUpdateCatalogVersion, entity.version());
    }
}

TEST_F(TableStructureTest, testDropColumn) {
    TableStructure entity;
    {
        proto::TableStructure inputProto;
        inputProto.set_table_name("tb1");
        inputProto.set_database_name("db1");
        inputProto.set_catalog_name("ct1");
        inputProto.set_version(3);
        inputProto.add_columns()->set_name("f1");
        inputProto.mutable_status()->set_code(proto::EntityStatus::PUBLISHED);
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_EQ(1UL, entity.columns().size());
    }

    ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.dropColumn("").code());
    ASSERT_EQ(1UL, entity.columns().size());

    ASSERT_EQ(Status::NOT_FOUND, entity.dropColumn("unknown").code());
    ASSERT_EQ(1UL, entity.columns().size());

    ASSERT_EQ(Status::OK, entity.dropColumn("f1").code());
    ASSERT_EQ(0UL, entity.columns().size());
    ASSERT_EQ(proto::EntityStatus::PENDING_PUBLISH, entity.status().code());
    ASSERT_EQ(kToUpdateCatalogVersion, entity.version());

    ASSERT_EQ(Status::NOT_FOUND, entity.dropColumn("unknown").code());
    ASSERT_EQ(0UL, entity.columns().size());
}

TEST_F(TableStructureTest, testCreateIndex) {
    TableStructure entity;
    {
        proto::TableStructure inputProto;
        inputProto.set_table_name("tb1");
        inputProto.set_database_name("db1");
        inputProto.set_catalog_name("ct1");
        inputProto.set_version(3);
        inputProto.add_indexes()->set_name("idx1");
        inputProto.mutable_status()->set_code(proto::EntityStatus::PUBLISHED);
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_EQ(1UL, entity.indexes().size());
    }
    {
        proto::TableStructure::Index index;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.createIndex(index).code());
        ASSERT_EQ(1UL, entity.indexes().size());
    }
    proto::TableStructure::Index index;
    index.set_name("idx2");
    {
        ASSERT_EQ(Status::OK, entity.createIndex(index).code());
        ASSERT_EQ(2UL, entity.indexes().size());
        ASSERT_TRUE(ProtoUtil::compareProto(index, entity.indexes()[1]));
        ASSERT_EQ(proto::EntityStatus::PENDING_PUBLISH, entity.status().code());
        ASSERT_EQ(kToUpdateCatalogVersion, entity.version());
    }
    {
        ASSERT_EQ(Status::DUPLICATE_ENTITY, entity.createIndex(index).code());
        ASSERT_EQ(2UL, entity.indexes().size());
    }
}

TEST_F(TableStructureTest, testUpdateIndex) {
    TableStructure entity;
    {
        proto::TableStructure inputProto;
        inputProto.set_table_name("tb1");
        inputProto.set_database_name("db1");
        inputProto.set_catalog_name("ct1");
        inputProto.set_version(3);
        inputProto.add_indexes()->set_name("idx1");
        inputProto.mutable_status()->set_code(proto::EntityStatus::PUBLISHED);
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_EQ(1UL, entity.indexes().size());
    }
    {
        proto::TableStructure::Index index;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.updateIndex(index).code());
        ASSERT_EQ(1UL, entity.indexes().size());
    }
    proto::TableStructure::Index index;
    index.set_name("idx1");
    {
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.updateIndex(index).code());
        ASSERT_EQ(1UL, entity.indexes().size());
        ASSERT_TRUE(ProtoUtil::compareProto(index, entity.indexes()[0]));
        ASSERT_EQ(proto::EntityStatus::PUBLISHED, entity.status().code());
        ASSERT_EQ(3, entity.version());
    }
    index.mutable_index_config()->add_index_fields()->set_field_name("f1");
    {
        ASSERT_EQ(Status::OK, entity.updateIndex(index).code());
        ASSERT_EQ(1UL, entity.indexes().size());
        ASSERT_TRUE(ProtoUtil::compareProto(index, entity.indexes()[0]));
        ASSERT_EQ(proto::EntityStatus::PENDING_PUBLISH, entity.status().code());
        ASSERT_EQ(kToUpdateCatalogVersion, entity.version());
    }
}

TEST_F(TableStructureTest, testDropIndex) {
    TableStructure entity;
    {
        proto::TableStructure inputProto;
        inputProto.set_table_name("tb1");
        inputProto.set_database_name("db1");
        inputProto.set_catalog_name("ct1");
        inputProto.set_version(3);
        inputProto.add_indexes()->set_name("idx1");
        inputProto.mutable_status()->set_code(proto::EntityStatus::PUBLISHED);
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_EQ(1UL, entity.indexes().size());
    }

    ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.dropIndex("").code());
    ASSERT_EQ(1UL, entity.indexes().size());

    ASSERT_EQ(Status::NOT_FOUND, entity.dropIndex("unknown").code());
    ASSERT_EQ(1UL, entity.indexes().size());

    ASSERT_EQ(Status::OK, entity.dropIndex("idx1").code());
    ASSERT_EQ(0UL, entity.indexes().size());
    ASSERT_EQ(proto::EntityStatus::PENDING_PUBLISH, entity.status().code());
    ASSERT_EQ(kToUpdateCatalogVersion, entity.version());

    ASSERT_EQ(Status::NOT_FOUND, entity.dropIndex("unknown").code());
    ASSERT_EQ(0UL, entity.indexes().size());
}

} // namespace catalog
