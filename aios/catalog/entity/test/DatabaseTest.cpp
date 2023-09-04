#include "catalog/entity/Database.h"

#include "gtest/gtest.h"
#include <string>
#include <vector>

#include "catalog/util/ProtoUtil.h"

namespace catalog {

class DatabaseTest : public ::testing::Test {};

auto createSimpleDatabaseProto() {
    proto::Database proto;
    proto.set_database_name("db1");
    proto.set_catalog_name("ct1");
    return proto;
}

auto createFullDatabaseProto() {
    proto::Database proto = createSimpleDatabaseProto();
    proto.mutable_status()->set_code(proto::EntityStatus::PUBLISHED);
    proto.set_version(3);
    proto.mutable_database_config()->set_version(1);
    {
        auto subProto = proto.add_tables();
        subProto->set_table_name("tb1");
        subProto->set_database_name("db1");
        subProto->set_catalog_name("ct1");
        subProto->mutable_table_config()->set_version(1);
        auto tableStructure = subProto->mutable_table_structure();
        tableStructure->set_table_name("tb1");
        tableStructure->set_database_name("db1");
        tableStructure->set_catalog_name("ct1");
        tableStructure->set_version(11);
        tableStructure->mutable_table_structure_config()->set_comment("xxx");
        subProto->set_version(2);
    }
    {
        auto subProto = proto.add_table_groups();
        subProto->set_table_group_name("tg1");
        subProto->set_database_name("db1");
        subProto->set_catalog_name("ct1");
        subProto->mutable_table_group_config()->set_version(1);
        subProto->set_version(2);
    }
    {
        auto subProto = proto.add_functions();
        subProto->set_function_name("func1");
        subProto->set_database_name("db1");
        subProto->set_catalog_name("ct1");
        subProto->mutable_function_config()->set_version(1);
        subProto->set_version(2);
    }
    proto.mutable_operation_meta()->set_created_time(123);
    return proto;
}

TEST_F(DatabaseTest, testConvert) {
    {
        proto::Database inputProto;
        Database entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::Database inputProto;
        inputProto.set_database_name("db1");
        Database entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    { // invalid table
        proto::Database inputProto;
        inputProto.set_database_name("db1");
        inputProto.set_catalog_name("ct1");
        {
            auto subProto = inputProto.add_tables();
            subProto->set_version(1);
        }
        Database entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    { // invalid table
        proto::Database inputProto;
        inputProto.set_database_name("db1");
        inputProto.set_catalog_name("ct1");
        {
            auto subProto = inputProto.add_tables();
            subProto->set_table_name("tb1");
            subProto->set_database_name("db2");
            subProto->set_version(1);
        }
        Database entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    { // invalid table
        proto::Database inputProto;
        inputProto.set_database_name("db1");
        inputProto.set_catalog_name("ct1");
        {
            auto subProto = inputProto.add_tables();
            subProto->set_table_name("tb1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct2");
            subProto->set_version(1);
        }
        Database entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    { // invalid table_group
        proto::Database inputProto;
        inputProto.set_database_name("db1");
        inputProto.set_catalog_name("ct1");
        {
            auto subProto = inputProto.add_table_groups();
            subProto->set_version(1);
        }
        Database entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    { // invalid table_group
        proto::Database inputProto = createSimpleDatabaseProto();
        {
            auto subProto = inputProto.add_table_groups();
            subProto->set_version(1);
            subProto->set_table_group_name("tg1");
            subProto->set_database_name("db2");
        }
        Database entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    { // invalid table_group
        proto::Database inputProto = createSimpleDatabaseProto();
        {
            auto subProto = inputProto.add_table_groups();
            subProto->set_version(1);
            subProto->set_table_group_name("tg1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct2");
        }
        Database entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    { // invalid table_group with invalid load_strategy
        proto::Database inputProto = createSimpleDatabaseProto();
        {
            auto subProto = inputProto.add_table_groups();
            subProto->set_version(1);
            subProto->set_table_group_name("tg1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
            {
                auto subSubProto = subProto->add_load_strategies();
                subSubProto->set_table_name("unknown");
                subSubProto->set_table_group_name("tg1");
                subSubProto->set_database_name("db1");
                subSubProto->set_catalog_name("ct1");
            }
        }
        Database entity;
        ASSERT_EQ(Status::NOT_FOUND, entity.fromProto(inputProto).code());
    }
    { // invalid function
        proto::Database inputProto;
        inputProto.set_database_name("db1");
        inputProto.set_catalog_name("ct1");
        {
            auto subProto = inputProto.add_functions();
            subProto->set_version(1);
        }
        Database entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    { // invalid function
        proto::Database inputProto = createSimpleDatabaseProto();
        {
            auto subProto = inputProto.add_functions();
            subProto->set_version(1);
            subProto->set_function_name("func1");
            subProto->set_database_name("db2");
        }
        Database entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    { // invalid function
        proto::Database inputProto = createSimpleDatabaseProto();
        {
            auto subProto = inputProto.add_functions();
            subProto->set_version(1);
            subProto->set_function_name("func1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct2");
        }
        Database entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        auto inputProto = createSimpleDatabaseProto();
        Database entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(proto::EntityStatus(), entity.status()));
        ASSERT_EQ(0, entity.version());
        ASSERT_EQ("db1", entity.id().databaseName);
        ASSERT_EQ("ct1", entity.id().catalogName);
        ASSERT_TRUE(ProtoUtil::compareProto(proto::DatabaseConfig(), entity.databaseConfig()));
        ASSERT_EQ(0UL, entity.tables().size());
        ASSERT_EQ(0UL, entity.tableGroups().size());
        ASSERT_EQ(0UL, entity.functions().size());

        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(inputProto, outputProto));
    }
    {
        auto inputProto = createFullDatabaseProto();
        Database entity;
        auto status = entity.fromProto(inputProto);
        ASSERT_EQ(Status::OK, status.code()) << status.message();
        ASSERT_TRUE(ProtoUtil::compareProto(inputProto.status(), entity.status()));
        ASSERT_EQ(3, entity.version());
        ASSERT_EQ("db1", entity.id().databaseName);
        ASSERT_EQ("ct1", entity.id().catalogName);
        ASSERT_TRUE(ProtoUtil::compareProto(inputProto.database_config(), entity.databaseConfig()));
        {
            ASSERT_EQ(1UL, entity.tables().size());
            const auto &subEntity = entity.tables().at("tb1");
            proto::Table expectedProto;
            ASSERT_EQ(Status::OK, subEntity->toProto(&expectedProto).code());
            ASSERT_TRUE(ProtoUtil::compareProto(inputProto.tables(0), expectedProto));
        }
        {
            ASSERT_EQ(1UL, entity.tableGroups().size());
            const auto &subEntity = entity.tableGroups().at("tg1");
            proto::TableGroup expectedProto;
            ASSERT_EQ(Status::OK, subEntity->toProto(&expectedProto).code());
            ASSERT_TRUE(ProtoUtil::compareProto(inputProto.table_groups(0), expectedProto));
        }
        {
            ASSERT_EQ(1UL, entity.functions().size());
            const auto &subEntity = entity.functions().at("func1");
            proto::Function expectedProto;
            ASSERT_EQ(Status::OK, subEntity->toProto(&expectedProto).code());
            ASSERT_TRUE(ProtoUtil::compareProto(inputProto.functions(0), expectedProto));
        }
        {
            proto::Database outputProto;
            ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
            ASSERT_TRUE(ProtoUtil::compareProto(inputProto, outputProto));
        }
        {
            proto::Database outputProto;
            ASSERT_EQ(Status::OK, entity.toProto(&outputProto, proto::DetailLevel::SUMMARY).code());
            auto expected = inputProto;
            {
                auto subProto = expected.mutable_tables(0);
                subProto->clear_database_name();
                subProto->clear_catalog_name();
                subProto->clear_table_config();
                subProto->clear_table_structure();
                subProto->clear_partitions();
                subProto->clear_operation_meta();
            }
            {
                auto subProto = expected.mutable_table_groups(0);
                subProto->clear_database_name();
                subProto->clear_catalog_name();
                subProto->clear_table_group_config();
                subProto->clear_load_strategies();
                subProto->clear_operation_meta();
            }
            {
                auto subProto = expected.mutable_functions(0);
                subProto->clear_database_name();
                subProto->clear_catalog_name();
                subProto->clear_function_config();
                subProto->clear_operation_meta();
            }
            std::string diff;
            ASSERT_TRUE(ProtoUtil::compareProto(expected, outputProto, &diff)) << diff;
        }
        {
            proto::Database outputProto;
            ASSERT_EQ(Status::OK, entity.toProto(&outputProto, proto::DetailLevel::MINIMAL).code());
            auto expected = inputProto;
            expected.clear_catalog_name();
            expected.clear_database_config();
            expected.clear_tables();
            expected.clear_table_groups();
            expected.clear_functions();
            expected.clear_operation_meta();
            std::string diff;
            ASSERT_TRUE(ProtoUtil::compareProto(expected, outputProto, &diff)) << diff;
        }
    }
    { // check toProto: tables sorted by name
        auto inputProto = createSimpleDatabaseProto();
        {
            auto subProto = inputProto.add_tables();
            subProto->set_table_name("tb2");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
            subProto->mutable_table_config()->set_version(1);
            auto tableStructure = subProto->mutable_table_structure();
            tableStructure->set_table_name("tb2");
            tableStructure->set_database_name("db1");
            tableStructure->set_catalog_name("ct1");
            tableStructure->set_version(11);
            subProto->set_version(2);
        }
        {
            auto subProto = inputProto.add_tables();
            subProto->set_table_name("tb1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
            subProto->mutable_table_config()->set_version(1);
            auto tableStructure = subProto->mutable_table_structure();
            tableStructure->set_table_name("tb1");
            tableStructure->set_database_name("db1");
            tableStructure->set_catalog_name("ct1");
            tableStructure->set_version(11);
            subProto->set_version(2);
        }
        Database entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        auto expectedProto = createSimpleDatabaseProto();
        expectedProto.add_tables()->CopyFrom(inputProto.tables(1));
        expectedProto.add_tables()->CopyFrom(inputProto.tables(0));
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
    { // check toProto: table_groups sorted by name
        auto inputProto = createSimpleDatabaseProto();
        {
            auto subProto = inputProto.add_table_groups();
            subProto->set_table_group_name("tg2");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
        }
        {
            auto subProto = inputProto.add_table_groups();
            subProto->set_table_group_name("tg1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
        }
        Database entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        auto expectedProto = createSimpleDatabaseProto();
        expectedProto.add_table_groups()->CopyFrom(inputProto.table_groups(1));
        expectedProto.add_table_groups()->CopyFrom(inputProto.table_groups(0));
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
    { // check toProto: functions sorted by name
        auto inputProto = createSimpleDatabaseProto();
        {
            auto subProto = inputProto.add_functions();
            subProto->set_function_name("func2");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
        }
        {
            auto subProto = inputProto.add_functions();
            subProto->set_function_name("func1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
        }
        Database entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        auto expectedProto = createSimpleDatabaseProto();
        expectedProto.add_functions()->CopyFrom(inputProto.functions(1));
        expectedProto.add_functions()->CopyFrom(inputProto.functions(0));
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
    { // duplicated tables
        auto inputProto = createSimpleDatabaseProto();
        {
            auto subProto = inputProto.add_tables();
            subProto->set_table_name("tb1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
            auto tableStructure = subProto->mutable_table_structure();
            tableStructure->set_table_name("tb1");
            tableStructure->set_database_name("db1");
            tableStructure->set_catalog_name("ct1");
            tableStructure->set_version(11);
        }
        inputProto.add_tables()->CopyFrom(inputProto.tables(0));
        Database entity;
        auto status = entity.fromProto(inputProto);
        ASSERT_EQ(Status::DUPLICATE_ENTITY, status.code()) << status.message();
    }
    { // duplicated table_groups
        auto inputProto = createSimpleDatabaseProto();
        {
            auto subProto = inputProto.add_table_groups();
            subProto->set_table_group_name("tg1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
        }
        inputProto.add_table_groups()->CopyFrom(inputProto.table_groups(0));
        Database entity;
        ASSERT_EQ(Status::DUPLICATE_ENTITY, entity.fromProto(inputProto).code());
    }
    { // table_group.load_strategy related table not exists
        auto inputProto = createSimpleDatabaseProto();
        {
            auto subProto = inputProto.add_table_groups();
            subProto->set_table_group_name("tg1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
            auto subSubProto = subProto->add_load_strategies();
            subSubProto->set_table_name("unknown");
            subSubProto->set_table_group_name("tg1");
            subSubProto->set_database_name("db1");
            subSubProto->set_catalog_name("ct1");
        }
        Database entity;
        ASSERT_EQ(Status::NOT_FOUND, entity.fromProto(inputProto).code());
    }
    { // duplicated functions
        auto inputProto = createSimpleDatabaseProto();
        {
            auto subProto = inputProto.add_functions();
            subProto->set_function_name("func1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
        }
        inputProto.add_functions()->CopyFrom(inputProto.functions(0));
        Database entity;
        ASSERT_EQ(Status::DUPLICATE_ENTITY, entity.fromProto(inputProto).code());
    }
}

TEST_F(DatabaseTest, testClone) {
    auto inputProto = createFullDatabaseProto();

    Database entity;
    ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

    auto newEntity = entity.clone();
    ASSERT_NE(nullptr, newEntity);

    proto::Database outputProto;
    ASSERT_EQ(Status::OK, newEntity->toProto(&outputProto).code());
    ASSERT_TRUE(ProtoUtil::compareProto(inputProto, outputProto));
}

TEST_F(DatabaseTest, testCompare) {
    {
        auto inputProto1 = createFullDatabaseProto();
        Database entity1;
        ASSERT_EQ(Status::OK, entity1.fromProto(inputProto1).code());

        auto inputProto2 = createFullDatabaseProto();
        Database entity2;
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
        auto inputProto2 = createFullDatabaseProto();
        inputProto2.set_version(10);
        inputProto2.mutable_database_config()->set_version(13);
        Database entity2;
        ASSERT_EQ(Status::OK, entity2.fromProto(inputProto2).code());

        DiffResult diffResult;
        ASSERT_EQ(Status::OK, entity2.compare(nullptr, diffResult).code());
        ASSERT_TRUE(diffResult.hasDiff());
        ASSERT_EQ(1UL, diffResult.databases.size());

        proto::DatabaseRecord expectedProto;
        expectedProto.set_database_name("db1");
        expectedProto.set_catalog_name("ct1");
        expectedProto.set_version(10);
        {
            auto subProto = expectedProto.mutable_database_config();
            subProto->set_version(13);
        }
        {
            auto subProto = expectedProto.add_table_records();
            subProto->set_entity_name("tb1");
            subProto->set_version(2);
        }
        {
            auto subProto = expectedProto.add_table_group_records();
            subProto->set_entity_name("tg1");
            subProto->set_version(2);
        }
        {
            auto subProto = expectedProto.add_function_records();
            subProto->set_entity_name("func1");
            subProto->set_version(2);
        }
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PUBLISHED);
        expectedProto.mutable_operation_meta()->set_created_time(123);
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, *diffResult.databases[0], &diff)) << diff;

        ASSERT_EQ(1UL, diffResult.tables.size());
        ASSERT_EQ("tb1", diffResult.tables[0]->table_name());
        ASSERT_EQ(1UL, diffResult.tableGroups.size());
        ASSERT_EQ("tg1", diffResult.tableGroups[0]->table_group_name());
        ASSERT_EQ(1UL, diffResult.functions.size());
        ASSERT_EQ("func1", diffResult.functions[0]->function_name());
        ASSERT_EQ(1UL, diffResult.tableStructures.size());
        ASSERT_EQ(11, diffResult.tableStructures[0]->version());
    }
    {
        auto inputProto1 = createFullDatabaseProto();
        Database entity1;
        ASSERT_EQ(Status::OK, entity1.fromProto(inputProto1).code());

        auto inputProto2 = createFullDatabaseProto();
        inputProto2.set_version(10);
        inputProto2.mutable_database_config()->set_version(13);
        Database entity2;
        ASSERT_EQ(Status::OK, entity2.fromProto(inputProto2).code());

        DiffResult diffResult;
        ASSERT_EQ(Status::OK, entity2.compare(&entity1, diffResult).code());
        ASSERT_TRUE(diffResult.hasDiff());
        ASSERT_EQ(1UL, diffResult.databases.size());

        proto::DatabaseRecord expectedProto;
        expectedProto.set_database_name("db1");
        expectedProto.set_catalog_name("ct1");
        expectedProto.set_version(10);
        {
            auto subProto = expectedProto.mutable_database_config();
            subProto->set_version(13);
        }
        {
            auto subProto = expectedProto.add_table_records();
            subProto->set_entity_name("tb1");
            subProto->set_version(2);
        }
        {
            auto subProto = expectedProto.add_table_group_records();
            subProto->set_entity_name("tg1");
            subProto->set_version(2);
        }
        {
            auto subProto = expectedProto.add_function_records();
            subProto->set_entity_name("func1");
            subProto->set_version(2);
        }
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PUBLISHED);
        expectedProto.mutable_operation_meta()->set_created_time(123);
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, *diffResult.databases[0], &diff)) << diff;

        ASSERT_EQ(0UL, diffResult.tables.size());
        ASSERT_EQ(0UL, diffResult.tableGroups.size());
        ASSERT_EQ(0UL, diffResult.functions.size());
        ASSERT_EQ(0UL, diffResult.tableStructures.size());
    }
    {
        auto inputProto1 = createFullDatabaseProto();
        Database entity1;
        ASSERT_EQ(Status::OK, entity1.fromProto(inputProto1).code());

        auto inputProto2 = createFullDatabaseProto();
        inputProto2.mutable_database_config()->set_version(11);
        inputProto2.set_version(10);
        {
            auto subProto = inputProto2.add_tables();
            subProto->set_table_name("tb2");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
            auto tableStructure = subProto->mutable_table_structure();
            tableStructure->set_table_name("tb2");
            tableStructure->set_database_name("db1");
            tableStructure->set_catalog_name("ct1");
            tableStructure->set_version(22);
            subProto->set_version(8);
        }
        {
            auto subProto = inputProto2.add_table_groups();
            subProto->set_table_group_name("tg2");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
            subProto->set_version(8);
        }
        {
            auto subProto = inputProto2.add_functions();
            subProto->set_function_name("func2");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
            subProto->set_version(8);
        }
        Database entity2;
        ASSERT_EQ(Status::OK, entity2.fromProto(inputProto2).code());

        DiffResult diffResult;
        ASSERT_EQ(Status::OK, entity2.compare(&entity1, diffResult).code());
        ASSERT_TRUE(diffResult.hasDiff());
        ASSERT_EQ(1UL, diffResult.databases.size());
        {
            proto::DatabaseRecord expectedProto;
            expectedProto.set_database_name("db1");
            expectedProto.set_catalog_name("ct1");
            expectedProto.set_version(10);
            expectedProto.mutable_database_config()->set_version(11);
            {
                auto subProto = expectedProto.add_table_records();
                subProto->set_entity_name("tb1");
                subProto->set_version(2);
            }
            {
                auto subProto = expectedProto.add_table_records();
                subProto->set_entity_name("tb2");
                subProto->set_version(8);
            }
            {
                auto subProto = expectedProto.add_table_group_records();
                subProto->set_entity_name("tg1");
                subProto->set_version(2);
            }
            {
                auto subProto = expectedProto.add_table_group_records();
                subProto->set_entity_name("tg2");
                subProto->set_version(8);
            }
            {
                auto subProto = expectedProto.add_function_records();
                subProto->set_entity_name("func1");
                subProto->set_version(2);
            }
            {
                auto subProto = expectedProto.add_function_records();
                subProto->set_entity_name("func2");
                subProto->set_version(8);
            }
            expectedProto.mutable_status()->set_code(proto::EntityStatus::PUBLISHED);
            expectedProto.mutable_operation_meta()->set_created_time(123);
            std::string diff;
            ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, *diffResult.databases[0], &diff)) << diff;
        }
        ASSERT_EQ(1UL, diffResult.tables.size());
        ASSERT_EQ("tb2", diffResult.tables[0]->table_name());
        ASSERT_EQ(8, diffResult.tables[0]->version());
        ASSERT_EQ(1UL, diffResult.tableStructures.size());
        ASSERT_EQ("tb2", diffResult.tableStructures[0]->table_name());
        ASSERT_EQ(22, diffResult.tableStructures[0]->version());
        ASSERT_EQ(1UL, diffResult.tableGroups.size());
        ASSERT_EQ("tg2", diffResult.tableGroups[0]->table_group_name());
        ASSERT_EQ(8, diffResult.tableGroups[0]->version());
        ASSERT_EQ(1UL, diffResult.functions.size());
        ASSERT_EQ("func2", diffResult.functions[0]->function_name());
        ASSERT_EQ(8, diffResult.functions[0]->version());
    }
}

TEST_F(DatabaseTest, testAlignVersion) {
    {
        proto::Database inputProto = createFullDatabaseProto();
        Database entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_EQ(3, entity.version());
        ASSERT_EQ(2, entity.tables().at("tb1")->version());
        ASSERT_EQ(11, entity.tables().at("tb1")->tableStructure()->version());
        ASSERT_EQ(2, entity.tableGroups().at("tg1")->version());
        ASSERT_EQ(2, entity.functions().at("func1")->version());
        entity.alignVersion(20);
        ASSERT_EQ(3, entity.version());
        ASSERT_EQ(2, entity.tables().at("tb1")->version());
        ASSERT_EQ(11, entity.tables().at("tb1")->tableStructure()->version());
        ASSERT_EQ(2, entity.tableGroups().at("tg1")->version());
        ASSERT_EQ(2, entity.functions().at("func1")->version());
    }
    {
        proto::Database inputProto = createFullDatabaseProto();
        inputProto.set_version(kToUpdateCatalogVersion);
        inputProto.mutable_tables(0)->set_version(kToUpdateCatalogVersion);
        inputProto.mutable_tables(0)->mutable_table_structure()->set_version(kToUpdateCatalogVersion);
        inputProto.mutable_table_groups(0)->set_version(kToUpdateCatalogVersion);
        inputProto.mutable_functions(0)->set_version(kToUpdateCatalogVersion);
        Database entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_EQ(kToUpdateCatalogVersion, entity.version());
        ASSERT_EQ(kToUpdateCatalogVersion, entity.tables().at("tb1")->version());
        ASSERT_EQ(kToUpdateCatalogVersion, entity.tables().at("tb1")->tableStructure()->version());
        ASSERT_EQ(kToUpdateCatalogVersion, entity.tableGroups().at("tg1")->version());
        ASSERT_EQ(kToUpdateCatalogVersion, entity.functions().at("func1")->version());
        entity.alignVersion(20);
        ASSERT_EQ(20, entity.version());
        ASSERT_EQ(20, entity.tables().at("tb1")->version());
        ASSERT_EQ(20, entity.tables().at("tb1")->tableStructure()->version());
        ASSERT_EQ(20, entity.tableGroups().at("tg1")->version());
        ASSERT_EQ(20, entity.functions().at("func1")->version());
    }
}

void addDefaultTableGroup(proto::Database &proto) {
    auto subProto = proto.add_table_groups();
    subProto->set_table_group_name("default");
    subProto->set_database_name(proto.database_name());
    subProto->set_catalog_name(proto.catalog_name());
    subProto->set_version(kToUpdateCatalogVersion);
    subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
}

TEST_F(DatabaseTest, testCreate) {
    { // simple case
        proto::Database inputProto = createSimpleDatabaseProto();
        Database entity;
        ASSERT_EQ(Status::OK, entity.create(inputProto).code());
        proto::TableGroup subRequest;
        subRequest.set_catalog_name("ct1");
        subRequest.set_database_name("db1");
        subRequest.set_table_group_name("default");
        ASSERT_EQ(Status::OK, entity.createTableGroup(subRequest).code());

        auto expectedProto = inputProto;
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        expectedProto.set_version(kToUpdateCatalogVersion);
        addDefaultTableGroup(expectedProto);
        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
    { // simple case with default table_group already assigned
        proto::Database inputProto = createSimpleDatabaseProto();
        {
            addDefaultTableGroup(inputProto);
            auto subProto = inputProto.mutable_table_groups(0);
            subProto->set_version(13);
            subProto->mutable_status()->set_code(proto::EntityStatus::PUBLISHED);
        }
        Database entity;
        ASSERT_EQ(Status::OK, entity.create(inputProto).code());

        auto expectedProto = inputProto;
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        expectedProto.set_version(kToUpdateCatalogVersion);
        {
            auto subProto = expectedProto.mutable_table_groups(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
    { // complex case
        proto::Database inputProto = createFullDatabaseProto();
        Database entity;
        ASSERT_EQ(Status::OK, entity.create(inputProto).code());

        auto expectedProto = createFullDatabaseProto();
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        expectedProto.set_version(kToUpdateCatalogVersion);
        {
            auto subProto = expectedProto.mutable_tables(0);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            subProto->set_version(kToUpdateCatalogVersion);
            {
                auto subSubProto = subProto->mutable_table_structure();
                subSubProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
                subSubProto->set_version(kToUpdateCatalogVersion);
            }
        }
        {
            // tableGroups保存时是排序的，因此需要手动处理一下
            auto tableGroup = expectedProto.table_groups(0);
            expectedProto.clear_table_groups();
            expectedProto.add_table_groups()->CopyFrom(tableGroup);
            for (auto &subProto : *expectedProto.mutable_table_groups()) {
                subProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
                subProto.set_version(kToUpdateCatalogVersion);
            }
        }
        {
            auto subProto = expectedProto.mutable_functions(0);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            subProto->set_version(kToUpdateCatalogVersion);
        }
        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
    { // failure
        proto::Database inputProto = createSimpleDatabaseProto();
        inputProto.add_tables();
        Database entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.create(inputProto).code());
    }
}

TEST_F(DatabaseTest, testDrop) {
    auto inputProto = createFullDatabaseProto();
    Database entity;
    ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
    ASSERT_NE(proto::EntityStatus::PENDING_DELETE, entity.status().code());
    ASSERT_NE(kToUpdateCatalogVersion, entity.version());
    ASSERT_EQ(Status::OK, entity.drop().code());
    ASSERT_EQ(proto::EntityStatus::PENDING_DELETE, entity.status().code());
    ASSERT_EQ(kToUpdateCatalogVersion, entity.version());
}

TEST_F(DatabaseTest, testUpdate) {
    { // success
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::Database expectedProto = createFullDatabaseProto();
        expectedProto.mutable_database_config()->set_version(22);
        auto request = expectedProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);

        ASSERT_EQ(Status::OK, entity.update(request).code());
        ASSERT_EQ(proto::EntityStatus::PENDING_PUBLISH, entity.status().code());
        ASSERT_EQ(kToUpdateCatalogVersion, entity.version());
        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
    { // no acutal update
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::Database request = createFullDatabaseProto();
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.update(request).code());
    }
}

TEST_F(DatabaseTest, testCreateTable) {
    { // failure
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        auto request = inputProto.tables(0);
        ASSERT_EQ(Status::DUPLICATE_ENTITY, entity.createTable(request).code());
    }
    { // success
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        auto request = inputProto.tables(0);
        request.set_table_name("tb2");
        request.mutable_table_structure()->set_table_name("tb2");
        auto status = entity.createTable(request);
        ASSERT_EQ(Status::OK, status.code()) << status.message();
        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.add_tables();
            subProto->CopyFrom(request);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            {
                auto subSubProto = subProto->mutable_table_structure();
                subSubProto->set_version(kToUpdateCatalogVersion);
                subSubProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            }
        }
        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
    { // success, auto add load_strategy to default table_group
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        {
            inputProto.clear_table_groups();
            addDefaultTableGroup(inputProto);
            auto tableGroup = inputProto.mutable_table_groups(0);
            tableGroup->set_version(22);
            tableGroup->mutable_status()->set_code(proto::EntityStatus::PUBLISHED);
        }
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        auto request = inputProto.tables(0);
        request.set_table_name("tb2");
        request.mutable_table_structure()->set_table_name("tb2");
        auto status = entity.createTable(request);
        ASSERT_EQ(Status::OK, status.code()) << status.message();
        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.add_tables();
            subProto->CopyFrom(request);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            {
                auto subSubProto = subProto->mutable_table_structure();
                subSubProto->set_version(kToUpdateCatalogVersion);
                subSubProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            }
        }
        {
            auto tableGroup = expectedProto.mutable_table_groups(0);
            tableGroup->set_version(kToUpdateCatalogVersion);
            tableGroup->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            auto loadStrategy = tableGroup->add_load_strategies();
            loadStrategy->set_table_name("tb2");
            loadStrategy->set_table_group_name(tableGroup->table_group_name());
            loadStrategy->set_database_name(tableGroup->database_name());
            loadStrategy->set_catalog_name(tableGroup->catalog_name());
            loadStrategy->set_version(kToUpdateCatalogVersion);
            loadStrategy->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(DatabaseTest, testDropTable) {
    { // failure
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        TableId request{"unknown", "db1", "ct1"};
        ASSERT_EQ(Status::NOT_FOUND, entity.dropTable(request).code());
    }
    { // success
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        TableId request{"tb1", "db1", "ct1"};
        ASSERT_EQ(Status::OK, entity.dropTable(request).code());
        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        expectedProto.clear_tables();
        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
    { // success
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        {
            auto subProto = inputProto.mutable_table_groups(0)->add_load_strategies();
            subProto->set_table_name("tb1");
            subProto->set_table_group_name("tg1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
        }
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        TableId request{"tb1", "db1", "ct1"};
        ASSERT_EQ(Status::OK, entity.dropTable(request).code());
        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        expectedProto.clear_tables();
        {
            auto subProto = expectedProto.mutable_table_groups(0);
            subProto->clear_load_strategies();
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(DatabaseTest, testUpdateTable) {
    { // failure: table not found
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        auto request = inputProto.tables(0);
        request.set_table_name("unknown");
        ASSERT_EQ(Status::NOT_FOUND, entity.updateTable(request).code());
    }
    { // failure: update failure
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        auto request = inputProto.tables(0);
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.updateTable(request).code());
    }
    { // success
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        auto request = inputProto.tables(0);
        request.mutable_table_config()->set_version(33);
        ASSERT_EQ(Status::OK, entity.updateTable(request).code());
        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_tables(0);
            subProto->mutable_table_config()->set_version(33);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

TEST_F(DatabaseTest, testUpdateTableStructure) {
    { // failure: table not found
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        auto request = inputProto.tables(0).table_structure();
        request.set_table_name("unknown");
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_EQ(Status::NOT_FOUND, entity.updateTableStructure(request).code());
    }
    { // failure: no actual update
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        auto request = inputProto.tables(0).table_structure();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.updateTableStructure(request).code());
    }
    { // success
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        auto request = inputProto.tables(0).table_structure();
        request.mutable_table_structure_config()->mutable_shard_info()->set_shard_count(16);
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_EQ(Status::OK, entity.updateTableStructure(request).code());
        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_tables(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            {
                auto subSubProto = subProto->mutable_table_structure();
                subSubProto->CopyFrom(request);
                subSubProto->set_version(kToUpdateCatalogVersion);
                subSubProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            }
        }
        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(DatabaseTest, testAddColumn) {
    { // failure: table not found
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::AddColumnRequest request;
        request.set_table_name("unknown");
        ASSERT_EQ(Status::NOT_FOUND, entity.addColumn(request).code());
    }
    { // failure: update failure
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::AddColumnRequest request;
        request.set_table_name("tb1");
        ASSERT_NE(Status::OK, entity.addColumn(request).code());
    }
    { // success
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::AddColumnRequest request;
        request.set_table_name("tb1");
        request.add_columns()->set_name("f1");
        request.add_columns()->set_name("f2");
        ASSERT_EQ(Status::OK, entity.addColumn(request).code());

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_tables(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            {
                auto subSubProto = subProto->mutable_table_structure();
                subSubProto->add_columns()->CopyFrom(request.columns(0));
                subSubProto->add_columns()->CopyFrom(request.columns(1));
                subSubProto->set_version(kToUpdateCatalogVersion);
                subSubProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            }
        }
        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(DatabaseTest, testUpdateColumn) {
    { // failure: table not found
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::UpdateColumnRequest request;
        request.set_table_name("unknown");
        ASSERT_EQ(Status::NOT_FOUND, entity.updateColumn(request).code());
    }
    { // failure: no column
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::UpdateColumnRequest request;
        request.set_table_name("tb1");
        ASSERT_NE(Status::OK, entity.updateColumn(request).code());
    }
    { // success
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        inputProto.mutable_tables(0)->mutable_table_structure()->add_columns()->set_name("f1");
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::UpdateColumnRequest request;
        {
            request.set_table_name("tb1");
            auto column = request.mutable_column();
            column->set_name("f1");
            column->set_comment("xxx");
        }
        ASSERT_EQ(Status::OK, entity.updateColumn(request).code());

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_tables(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            {
                auto subSubProto = subProto->mutable_table_structure();
                subSubProto->mutable_columns(0)->CopyFrom(request.column());
                subSubProto->set_version(kToUpdateCatalogVersion);
                subSubProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            }
        }
        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(DatabaseTest, testDropColumn) {
    { // failure: table not found
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::DropColumnRequest request;
        request.set_table_name("unknown");
        ASSERT_EQ(Status::NOT_FOUND, entity.dropColumn(request).code());
    }
    { // failure: drop failure
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::DropColumnRequest request;
        request.set_table_name("tb1");
        request.add_column_names("unknown");
        ASSERT_NE(Status::OK, entity.dropColumn(request).code());
    }
    { // success
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        inputProto.mutable_tables(0)->mutable_table_structure()->add_columns()->set_name("f1");
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::DropColumnRequest request;
        request.set_table_name("tb1");
        request.add_column_names("f1");
        ASSERT_EQ(Status::OK, entity.dropColumn(request).code());

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_tables(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            {
                auto subSubProto = subProto->mutable_table_structure();
                subSubProto->clear_columns();
                subSubProto->set_version(kToUpdateCatalogVersion);
                subSubProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            }
        }
        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(DatabaseTest, testCreateIndex) {
    { // failure: table not found
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::CreateIndexRequest request;
        request.set_table_name("unknown");
        ASSERT_EQ(Status::NOT_FOUND, entity.createIndex(request).code());
    }
    { // failure: create failure
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::CreateIndexRequest request;
        request.set_table_name("tb1");
        ASSERT_NE(Status::OK, entity.createIndex(request).code());
    }
    { // success
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::CreateIndexRequest request;
        request.set_table_name("tb1");
        request.add_indexes()->set_name("f1");
        request.add_indexes()->set_name("f2");
        ASSERT_EQ(Status::OK, entity.createIndex(request).code());

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_tables(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            {
                auto subSubProto = subProto->mutable_table_structure();
                subSubProto->add_indexes()->CopyFrom(request.indexes(0));
                subSubProto->add_indexes()->CopyFrom(request.indexes(1));
                subSubProto->set_version(kToUpdateCatalogVersion);
                subSubProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            }
        }
        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(DatabaseTest, testUpdateIndex) {
    { // failure: table not found
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::UpdateIndexRequest request;
        request.set_table_name("unknown");
        ASSERT_EQ(Status::NOT_FOUND, entity.updateIndex(request).code());
    }
    { // failure: no column
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::UpdateIndexRequest request;
        request.set_table_name("tb1");
        ASSERT_NE(Status::OK, entity.updateIndex(request).code());
    }
    { // success
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        inputProto.mutable_tables(0)->mutable_table_structure()->add_indexes()->set_name("f1");
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::UpdateIndexRequest request;
        {
            request.set_table_name("tb1");
            auto column = request.mutable_index();
            column->set_name("f1");
            column->set_index_type(proto::TableStructure::Index::NUMBER);
        }
        ASSERT_EQ(Status::OK, entity.updateIndex(request).code());

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_tables(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            {
                auto subSubProto = subProto->mutable_table_structure();
                subSubProto->mutable_indexes(0)->CopyFrom(request.index());
                subSubProto->set_version(kToUpdateCatalogVersion);
                subSubProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            }
        }
        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(DatabaseTest, testDropIndex) {
    { // failure: table not found
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::DropIndexRequest request;
        request.set_table_name("unknown");
        ASSERT_EQ(Status::NOT_FOUND, entity.dropIndex(request).code());
    }
    { // failure: exec failure
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::DropIndexRequest request;
        request.set_table_name("tb1");
        request.add_index_names("f1");
        ASSERT_NE(Status::OK, entity.dropIndex(request).code());
    }
    { // success
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        inputProto.mutable_tables(0)->mutable_table_structure()->add_indexes()->set_name("f1");
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::DropIndexRequest request;
        request.set_table_name("tb1");
        request.add_index_names("f1");
        ASSERT_EQ(Status::OK, entity.dropIndex(request).code());

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_tables(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            {
                auto subSubProto = subProto->mutable_table_structure();
                subSubProto->clear_indexes();
                subSubProto->set_version(kToUpdateCatalogVersion);
                subSubProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            }
        }
        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(DatabaseTest, testCreatePartition) {
    { // failure: invalid partition_type
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::CreatePartitionRequest request;
        request.mutable_partition()->set_table_name("unknown");
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.createPartition(request).code());
    }
    { // failure: table not found
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::CreatePartitionRequest request;
        request.mutable_partition()->set_table_name("unknown");
        request.mutable_partition()->set_partition_type(proto::PartitionType::TABLE_PARTITION);
        ASSERT_EQ(Status::NOT_FOUND, entity.createPartition(request).code());
    }
    { // create partition for table failure
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::CreatePartitionRequest request;
        {
            auto subProto = request.mutable_partition();
            subProto->set_partition_name("part1");
            subProto->set_table_name("tb1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
            subProto->set_partition_type(proto::PartitionType::TABLE_PARTITION);
            subProto->mutable_table_structure()->set_version(11);
        }
        ASSERT_NE(Status::OK, entity.createPartition(request).code());
    }
    { // create partition for table success
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::CreatePartitionRequest request;
        {
            auto subProto = request.mutable_partition();
            subProto->set_partition_name("part1");
            subProto->set_table_name("tb1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
            subProto->set_partition_type(proto::PartitionType::TABLE_PARTITION);
        }
        ASSERT_EQ(Status::OK, entity.createPartition(request).code());

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_tables(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            {
                auto subSubProto = subProto->add_partitions();
                subSubProto->CopyFrom(request.partition());
                subSubProto->mutable_table_structure()->CopyFrom(subProto->table_structure());
                subSubProto->set_version(kToUpdateCatalogVersion);
                subSubProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            }
        }
        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

TEST_F(DatabaseTest, testUpdatePartition) {
    { // failure: invalid partition_type
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::UpdatePartitionRequest request;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.updatePartition(request).code());
    }
    { // failure: table not found
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::UpdatePartitionRequest request;
        request.mutable_partition()->set_table_name("unknown");
        request.mutable_partition()->set_partition_type(proto::PartitionType::TABLE_PARTITION);
        ASSERT_EQ(Status::NOT_FOUND, entity.updatePartition(request).code());
    }
    { // failure: table partition not found
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::UpdatePartitionRequest request;
        {
            auto subProto = request.mutable_partition();
            subProto->set_table_name("tb1");
            subProto->set_partition_name("unknown");
        }
        ASSERT_NE(Status::OK, entity.updatePartition(request).code());
    }
    { // success update table partition
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        {
            auto subProto = inputProto.mutable_tables(0)->add_partitions();
            subProto->set_partition_name("part1");
            subProto->set_table_name("tb1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
            subProto->set_partition_type(proto::PartitionType::TABLE_PARTITION);
            subProto->mutable_partition_config()->set_version(2);
        }
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::UpdatePartitionRequest request;
        {
            auto subProto = request.mutable_partition();
            subProto->CopyFrom(inputProto.tables(0).partitions(0));
            subProto->mutable_partition_config()->set_version(11);
        }
        ASSERT_EQ(Status::OK, entity.updatePartition(request).code());

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_tables(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            {
                auto subSubProto = subProto->mutable_partitions(0);
                subSubProto->CopyFrom(request.partition());
                subSubProto->set_version(kToUpdateCatalogVersion);
                subSubProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            }
        }
        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

TEST_F(DatabaseTest, testUpdatePartitionTableStructure) {
    { // failure: table not found
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::UpdatePartitionTableStructureRequest request;
        request.set_partition_name("unknown");
        request.set_table_name("unknown");
        ASSERT_EQ(Status::NOT_FOUND, entity.updatePartitionTableStructure(request).code());
    }
    { // failure: partition not found
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::UpdatePartitionTableStructureRequest request;
        request.set_partition_name("unknown");
        request.set_table_name("tb1");
        ASSERT_NE(Status::OK, entity.updatePartitionTableStructure(request).code());
    }
    { // success
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        {
            auto subProto = inputProto.mutable_tables(0);
            {
                auto subSubProto = subProto->add_partitions();
                subSubProto->set_partition_name("part1");
                subSubProto->set_table_name("tb1");
                subSubProto->set_database_name("db1");
                subSubProto->set_catalog_name("ct1");
                subSubProto->set_partition_type(proto::PartitionType::TABLE_PARTITION);
                subSubProto->mutable_table_structure()->CopyFrom(subProto->table_structure());
            }
            subProto->mutable_table_structure()->add_columns()->set_name("f1");
        }
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::UpdatePartitionTableStructureRequest request;
        request.set_partition_name("part1");
        request.set_table_name("tb1");
        request.set_database_name("tdb1");
        request.set_catalog_name("ct1");
        ASSERT_EQ(Status::OK, entity.updatePartitionTableStructure(request).code());

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_tables(0);
            {
                auto subSubProto = subProto->mutable_partitions(0);
                subSubProto->mutable_table_structure()->CopyFrom(subProto->table_structure());
                subSubProto->set_version(kToUpdateCatalogVersion);
                subSubProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            }
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(DatabaseTest, testDropPartition) {
    { // failure: invalid partition_type
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::DropPartitionRequest request;
        request.set_partition_name("part1");
        request.set_table_name("tb1");
        request.set_database_name("db1");
        request.set_catalog_name("ct1");
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.dropPartition(request).code());
    }
    { // failure: table not found
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::DropPartitionRequest request;
        request.set_partition_name("part1");
        request.set_table_name("unknown");
        request.set_database_name("db1");
        request.set_catalog_name("ct1");
        request.set_partition_type(proto::PartitionType::TABLE_PARTITION);
        ASSERT_EQ(Status::NOT_FOUND, entity.dropPartition(request).code());
    }
    { // failure: table partition not found
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::DropPartitionRequest request;
        request.set_partition_name("part1");
        request.set_table_name("tb1");
        request.set_database_name("db1");
        request.set_catalog_name("ct1");
        request.set_partition_type(proto::PartitionType::TABLE_PARTITION);
        ASSERT_NE(Status::OK, entity.dropPartition(request).code());
    }
}

TEST_F(DatabaseTest, testCreateTableGroup) {
    { // failure: invalid request
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::TableGroup request;
        request.set_table_group_name("tg1");
        request.set_database_name("db1");
        request.set_catalog_name("ct1");
        ASSERT_EQ(Status::DUPLICATE_ENTITY, entity.createTableGroup(request).code());
    }
    { // failure: table_group create failure
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::TableGroup request;
        request.set_table_group_name("tg2");
        request.set_database_name("db1");
        request.set_catalog_name("ct1");
        {
            auto subProto = request.add_load_strategies();
            subProto->set_table_name("tb1");
            subProto->set_table_group_name("unknown");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
        }
        ASSERT_NE(Status::OK, entity.createTableGroup(request).code());
    }
    { // success create table_group
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::TableGroup request;
        request.set_table_group_name("tg2");
        request.set_database_name("db1");
        request.set_catalog_name("ct1");
        ASSERT_EQ(Status::OK, entity.createTableGroup(request).code());

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.add_table_groups();
            subProto->CopyFrom(request);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(DatabaseTest, testUpdateTableGroup) {
    { // failure: invalid request
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::TableGroup request = inputProto.table_groups(0);
        request.set_table_group_name("unknown");
        ASSERT_EQ(Status::NOT_FOUND, entity.updateTableGroup(request).code());
    }
    { // table_group update failure
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::TableGroup request = inputProto.table_groups(0);
        ASSERT_NE(Status::OK, entity.updateTableGroup(request).code());
    }
    { // success update table_group
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::TableGroup request = inputProto.table_groups(0);
        request.mutable_table_group_config()->set_version(33);
        ASSERT_EQ(Status::OK, entity.updateTableGroup(request).code());

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_table_groups(0);
            subProto->CopyFrom(request);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(DatabaseTest, testDropTableGroup) {
    { // failure: invalid request
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        TableGroupId request{"unknown", "db1", "ct1"};
        ASSERT_EQ(Status::NOT_FOUND, entity.dropTableGroup(request).code());
    }
    { // success drop table_group
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        TableGroupId request{"tg1", "db1", "ct1"};
        ASSERT_EQ(Status::OK, entity.dropTableGroup(request).code());

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        expectedProto.clear_table_groups();
        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(DatabaseTest, testCreateLoadStrategy) {
    { // failure: table_group not exists
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::LoadStrategy request;
        request.set_table_name("tb1");
        request.set_table_group_name("unknown");
        request.set_database_name("db1");
        request.set_catalog_name("ct1");
        ASSERT_EQ(Status::NOT_FOUND, entity.createLoadStrategy(request).code());
    }
    { // failure: table not exists
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::LoadStrategy request;
        request.set_table_name("unknown");
        request.set_table_group_name("tg1");
        request.set_database_name("db1");
        request.set_catalog_name("ct1");
        ASSERT_EQ(Status::NOT_FOUND, entity.createLoadStrategy(request).code());
    }
    { // failure: load_strategy create failure
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        {
            auto subProto = inputProto.mutable_table_groups(0)->add_load_strategies();
            subProto->set_table_name("tb1");
            subProto->set_table_group_name("tg1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
        }
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::LoadStrategy request;
        request.set_table_name("tb1");
        request.set_table_group_name("tg1");
        request.set_database_name("db1");
        request.set_catalog_name("ct1");
        ASSERT_NE(Status::OK, entity.createLoadStrategy(request).code());
    }
    { // success create load_strategy
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::LoadStrategy request;
        request.set_table_name("tb1");
        request.set_table_group_name("tg1");
        request.set_database_name("db1");
        request.set_catalog_name("ct1");
        ASSERT_EQ(Status::OK, entity.createLoadStrategy(request).code());

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_table_groups(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            {
                auto subSubProto = subProto->add_load_strategies();
                subSubProto->CopyFrom(request);
                subSubProto->set_version(kToUpdateCatalogVersion);
                subSubProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            }
        }
        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(DatabaseTest, testUpdateLoadStrategy) {
    { // failure: invalid request
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::LoadStrategy request;
        request.set_table_name("tb1");
        request.set_table_group_name("unknown");
        request.set_database_name("db1");
        request.set_catalog_name("ct1");
        ASSERT_EQ(Status::NOT_FOUND, entity.updateLoadStrategy(request).code());
    }
    { // function update failure
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        {
            auto subProto = inputProto.mutable_table_groups(0)->add_load_strategies();
            subProto->set_table_name("tb1");
            subProto->set_table_group_name("tg1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
        }
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::LoadStrategy request = inputProto.table_groups(0).load_strategies(0);
        ASSERT_NE(Status::OK, entity.updateLoadStrategy(request).code());
    }
    { // success update load_strategy
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        {
            auto subProto = inputProto.mutable_table_groups(0)->add_load_strategies();
            subProto->set_table_name("tb1");
            subProto->set_table_group_name("tg1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
        }
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::LoadStrategy request = inputProto.table_groups(0).load_strategies(0);
        request.mutable_load_strategy_config()->set_version(33);
        ASSERT_EQ(Status::OK, entity.updateLoadStrategy(request).code());

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_table_groups(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            {
                auto subSubProto = subProto->mutable_load_strategies(0);
                subSubProto->CopyFrom(request);
                subSubProto->set_version(kToUpdateCatalogVersion);
                subSubProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            }
        }
        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(DatabaseTest, testDropLoadStrategy) {
    { // failure: invalid request
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        LoadStrategyId request{"tb1", "unknown", "db1", "ct1"};
        ASSERT_EQ(Status::NOT_FOUND, entity.dropLoadStrategy(request).code());
    }
    { // success drop load_strategy
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        {
            auto subProto = inputProto.mutable_table_groups(0)->add_load_strategies();
            subProto->set_table_name("tb1");
            subProto->set_table_group_name("tg1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
        }
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        LoadStrategyId request{"tb1", "tg1", "db1", "ct1"};
        ASSERT_EQ(Status::OK, entity.dropLoadStrategy(request).code());

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_table_groups(0);
            subProto->clear_load_strategies();
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(DatabaseTest, testCreateFunction) {
    { // failure: invalid request
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::Function request = inputProto.functions(0);
        ASSERT_EQ(Status::DUPLICATE_ENTITY, entity.createFunction(request).code());
    }
    { // success create function
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::Function request = inputProto.functions(0);
        request.set_function_name("func2");
        request.mutable_function_config()->set_version(333);
        ASSERT_EQ(Status::OK, entity.createFunction(request).code());

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.add_functions();
            subProto->CopyFrom(request);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(DatabaseTest, testUpdateFunction) {
    { // failure: invalid request
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::Function request = inputProto.functions(0);
        request.set_function_name("unknown");
        ASSERT_EQ(Status::NOT_FOUND, entity.updateFunction(request).code());
    }
    { // function update failure
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::Function request = inputProto.functions(0);
        ASSERT_NE(Status::OK, entity.updateFunction(request).code());
    }
    { // success update function
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::Function request = inputProto.functions(0);
        request.mutable_function_config()->set_version(33);
        ASSERT_EQ(Status::OK, entity.updateFunction(request).code());

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_functions(0);
            subProto->CopyFrom(request);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}
TEST_F(DatabaseTest, testDropFunction) {
    { // failure: invalid request
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        FunctionId request{"unknown", "db1", "ct1"};
        ASSERT_EQ(Status::NOT_FOUND, entity.dropFunction(request).code());
    }
    { // success drop function
        Database entity;
        proto::Database inputProto = createFullDatabaseProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        FunctionId request{"func1", "db1", "ct1"};
        ASSERT_EQ(Status::OK, entity.dropFunction(request).code());

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        expectedProto.clear_functions();
        proto::Database outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(DatabaseTest, testGetTable) {
    Database entity;
    proto::Database inputProto = createFullDatabaseProto();
    ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
    Table *subEntity = nullptr;
    ASSERT_EQ(Status::NOT_FOUND, entity.getTable("unknown", subEntity).code());
    ASSERT_EQ(nullptr, subEntity);
    ASSERT_EQ(Status::OK, entity.getTable("tb1", subEntity).code());
    ASSERT_NE(nullptr, subEntity);
}

TEST_F(DatabaseTest, testListTable) {
    Database entity;
    ASSERT_EQ(std::vector<std::string>{}, entity.listTable());
    proto::Database inputProto = createFullDatabaseProto();
    ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
    ASSERT_EQ(std::vector<std::string>{"tb1"}, entity.listTable());
}

TEST_F(DatabaseTest, testGetTableGroup) {
    Database entity;
    proto::Database inputProto = createFullDatabaseProto();
    ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
    TableGroup *subEntity = nullptr;
    ASSERT_EQ(Status::NOT_FOUND, entity.getTableGroup("unknown", subEntity).code());
    ASSERT_EQ(nullptr, subEntity);
    ASSERT_EQ(Status::OK, entity.getTableGroup("tg1", subEntity).code());
    ASSERT_NE(nullptr, subEntity);
}

TEST_F(DatabaseTest, testListTableGroup) {
    Database entity;
    ASSERT_EQ(std::vector<std::string>{}, entity.listTableGroup());
    proto::Database inputProto = createFullDatabaseProto();
    ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
    ASSERT_EQ(std::vector<std::string>{"tg1"}, entity.listTableGroup());
}

TEST_F(DatabaseTest, testGetFunction) {
    Database entity;
    proto::Database inputProto = createFullDatabaseProto();
    ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
    Function *subEntity = nullptr;
    ASSERT_EQ(Status::NOT_FOUND, entity.getFunction("unknown", subEntity).code());
    ASSERT_EQ(nullptr, subEntity);
    ASSERT_EQ(Status::OK, entity.getFunction("func1", subEntity).code());
    ASSERT_NE(nullptr, subEntity);
}

TEST_F(DatabaseTest, testListFunction) {
    Database entity;
    ASSERT_EQ(std::vector<std::string>{}, entity.listFunction());
    proto::Database inputProto = createFullDatabaseProto();
    ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
    ASSERT_EQ(std::vector<std::string>{"func1"}, entity.listFunction());
}

} // namespace catalog
