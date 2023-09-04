#include "catalog/entity/Catalog.h"

#include "gtest/gtest.h"
#include <stdio.h>
#include <string>

#include "catalog/util/ProtoUtil.h"

namespace catalog {

class CatalogTest : public ::testing::Test {};

auto createSimpleCatalogProto() {
    proto::Catalog proto;
    proto.set_catalog_name("ct1");
    proto.set_version(5);
    proto.mutable_status()->set_code(proto::EntityStatus::PUBLISHED);
    return proto;
}

auto createFullCatalogProto() {
    proto::Catalog proto = createSimpleCatalogProto();
    proto.mutable_catalog_config()->set_version(1);
    {
        auto subProto = proto.add_databases();
        subProto->set_database_name("db1");
        subProto->set_catalog_name("ct1");
        subProto->mutable_database_config()->set_version(22);
        subProto->set_version(2);
    }
    proto.mutable_operation_meta()->set_created_time(123);
    return proto;
}

extern void addDefaultTableGroup(proto::Database &proto);

void fillTableStructureProto(proto::TableStructure &proto) {
    proto.set_table_name("tb1");
    proto.set_database_name("db1");
    proto.set_catalog_name("ct1");
}

void fillTableProto(proto::Table &proto) {
    proto.set_table_name("tb1");
    proto.set_database_name("db1");
    proto.set_catalog_name("ct1");
    auto subProto = proto.mutable_table_structure();
    fillTableStructureProto(*subProto);
}

void fillPartitonProto(proto::Partition &proto, proto::PartitionType::Code partitionType) {
    proto.set_partition_name("part1");
    proto.set_table_name("tb1");
    proto.set_database_name("db1");
    proto.set_catalog_name("ct1");
    proto.set_partition_type(partitionType);
}

void fillTableGroupProto(proto::TableGroup &proto) {
    proto.set_table_group_name("tg1");
    proto.set_database_name("db1");
    proto.set_catalog_name("ct1");
}

void fillLoadStrategyProto(proto::LoadStrategy &proto) {
    proto.set_table_name("tb1");
    proto.set_table_group_name("tg1");
    proto.set_database_name("db1");
    proto.set_catalog_name("ct1");
}

void fillFunctionProto(proto::Function &proto) {
    proto.set_function_name("func1");
    proto.set_database_name("db1");
    proto.set_catalog_name("ct1");
}

TEST_F(CatalogTest, testConvert) {
    {
        proto::Catalog inputProto;
        Catalog entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::Catalog inputProto = createSimpleCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(inputProto.status(), entity.status()));
        ASSERT_EQ(5, entity.version());
        ASSERT_EQ("ct1", entity.catalogName());
        ASSERT_EQ(0UL, entity.databases().size());
        ASSERT_TRUE(ProtoUtil::compareProto(inputProto.catalog_config(), entity.catalogConfig()));
        proto::Catalog outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(inputProto, outputProto));
    }
    {
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(inputProto.status(), entity.status()));
        ASSERT_EQ(5, entity.version());
        ASSERT_EQ("ct1", entity.catalogName());
        ASSERT_TRUE(ProtoUtil::compareProto(inputProto.catalog_config(), entity.catalogConfig()));
        ASSERT_EQ(1UL, entity.databases().size());
        {
            auto expectedProto = inputProto.databases(0);
            proto::Database outputProto;
            ASSERT_EQ(Status::OK, entity.databases().at("db1")->toProto(&outputProto).code());
            ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
        }
        {
            proto::Catalog outputProto;
            ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
            ASSERT_TRUE(ProtoUtil::compareProto(inputProto, outputProto));
        }
        {
            proto::Catalog outputProto;
            ASSERT_EQ(Status::OK, entity.toProto(&outputProto, proto::DetailLevel::SUMMARY).code());
            auto expected = inputProto;
            {
                auto subProto = expected.mutable_databases(0);
                subProto->clear_catalog_name();
                subProto->clear_database_config();
                subProto->clear_operation_meta();
            }
            std::string diff;
            ASSERT_TRUE(ProtoUtil::compareProto(expected, outputProto, &diff)) << diff;
        }
        {
            proto::Catalog outputProto;
            ASSERT_EQ(Status::OK, entity.toProto(&outputProto, proto::DetailLevel::MINIMAL).code());
            auto expected = inputProto;
            expected.clear_catalog_name();
            expected.clear_catalog_config();
            expected.clear_databases();
            expected.clear_operation_meta();
            std::string diff;
            ASSERT_TRUE(ProtoUtil::compareProto(expected, outputProto, &diff)) << diff;
        }
    }
}

TEST_F(CatalogTest, testClone) {
    proto::Catalog inputProto = createSimpleCatalogProto();

    Catalog entity;
    ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

    auto newEntity = entity.clone();
    ASSERT_NE(nullptr, newEntity);

    proto::Catalog outputProto;
    ASSERT_EQ(Status::OK, newEntity->toProto(&outputProto).code());
    ASSERT_TRUE(ProtoUtil::compareProto(inputProto, outputProto));
}

TEST_F(CatalogTest, testCompare) {
    {
        proto::Catalog inputProto = createSimpleCatalogProto();
        Catalog entity1;
        ASSERT_EQ(Status::OK, entity1.fromProto(inputProto).code());
        Catalog entity2;
        ASSERT_EQ(Status::OK, entity2.fromProto(inputProto).code());

        DiffResult diffResult;
        ASSERT_EQ(Status::OK, entity2.compare(&entity1, diffResult).code());
        ASSERT_FALSE(diffResult.hasDiff());
    }
    { // compare with nullptr
        proto::Catalog inputProto2 = createSimpleCatalogProto();
        inputProto2.set_version(7);
        inputProto2.mutable_catalog_config()->set_version(111);
        Catalog entity2;
        ASSERT_EQ(Status::OK, entity2.fromProto(inputProto2).code());

        DiffResult diffResult;
        ASSERT_EQ(Status::OK, entity2.compare(nullptr, diffResult).code());
        ASSERT_TRUE(diffResult.hasDiff());
        ASSERT_NE(nullptr, diffResult.catalog);

        proto::CatalogRecord expectedProto;
        expectedProto.set_catalog_name("ct1");
        expectedProto.set_version(inputProto2.version());
        expectedProto.mutable_catalog_config()->CopyFrom(inputProto2.catalog_config());
        expectedProto.mutable_status()->CopyFrom(inputProto2.status());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, *diffResult.catalog, &diff)) << diff;
        ASSERT_EQ(0UL, diffResult.databases.size());
    }
    { // compare with other
        proto::Catalog inputProto1 = createSimpleCatalogProto();
        Catalog entity1;
        ASSERT_EQ(Status::OK, entity1.fromProto(inputProto1).code());

        proto::Catalog inputProto2 = createSimpleCatalogProto();
        inputProto2.set_version(7);
        inputProto2.mutable_catalog_config()->set_version(111);
        Catalog entity2;
        ASSERT_EQ(Status::OK, entity2.fromProto(inputProto2).code());

        DiffResult diffResult;
        ASSERT_EQ(Status::OK, entity2.compare(&entity1, diffResult).code());
        ASSERT_TRUE(diffResult.hasDiff());
        ASSERT_NE(nullptr, diffResult.catalog);

        proto::CatalogRecord expectedProto;
        expectedProto.set_catalog_name("ct1");
        expectedProto.set_version(inputProto2.version());
        expectedProto.mutable_catalog_config()->CopyFrom(inputProto2.catalog_config());
        expectedProto.mutable_status()->CopyFrom(inputProto2.status());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, *diffResult.catalog));
        ASSERT_EQ(0UL, diffResult.databases.size());
    }
    { // compare with other
        proto::Catalog inputProto1 = createSimpleCatalogProto();
        Catalog entity1;
        ASSERT_EQ(Status::OK, entity1.fromProto(inputProto1).code());

        proto::Catalog inputProto2 = createFullCatalogProto();
        inputProto2.set_version(7);
        inputProto2.mutable_catalog_config()->set_version(111);
        Catalog entity2;
        ASSERT_EQ(Status::OK, entity2.fromProto(inputProto2).code());

        DiffResult diffResult;
        ASSERT_EQ(Status::OK, entity2.compare(&entity1, diffResult).code());
        ASSERT_TRUE(diffResult.hasDiff());
        ASSERT_NE(nullptr, diffResult.catalog);

        proto::CatalogRecord expectedProto;
        expectedProto.set_catalog_name("ct1");
        expectedProto.set_version(inputProto2.version());
        expectedProto.mutable_catalog_config()->CopyFrom(inputProto2.catalog_config());
        expectedProto.mutable_status()->CopyFrom(inputProto2.status());
        expectedProto.mutable_operation_meta()->set_created_time(123);
        {
            auto subProto = expectedProto.add_database_records();
            subProto->set_entity_name("db1");
            subProto->set_version(2);
        }
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, *diffResult.catalog, &diff)) << diff;
        {
            ASSERT_EQ(1UL, diffResult.databases.size());
            proto::DatabaseRecord expectedSubProto;
            expectedSubProto.set_database_name("db1");
            expectedSubProto.set_catalog_name("ct1");
            expectedSubProto.set_version(inputProto2.databases(0).version());
            expectedSubProto.mutable_database_config()->CopyFrom(inputProto2.databases(0).database_config());
            expectedSubProto.mutable_status()->CopyFrom(inputProto2.databases(0).status());
            ASSERT_TRUE(ProtoUtil::compareProto(expectedSubProto, *diffResult.databases[0]));
        }
    }
}

TEST_F(CatalogTest, testAlignVersion) {
    {
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_EQ(5, entity.version());
        ASSERT_EQ(2, inputProto.databases(0).version());
        entity.alignVersion(20);
        ASSERT_EQ(5, entity.version());
        ASSERT_EQ(2, entity.databases().at("db1")->version());
    }
    {
        proto::Catalog inputProto = createFullCatalogProto();
        inputProto.set_version(kToUpdateCatalogVersion);
        inputProto.mutable_databases(0)->set_version(kToUpdateCatalogVersion);
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_EQ(kToUpdateCatalogVersion, entity.version());
        ASSERT_EQ(kToUpdateCatalogVersion, inputProto.databases(0).version());
        entity.alignVersion(20);
        ASSERT_EQ(20, entity.version());
        ASSERT_EQ(20, entity.databases().at("db1")->version());
    }
}

TEST_F(CatalogTest, testCreate) {
    { // failure
        proto::Catalog inputProto;
        Catalog entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.create(inputProto).code());
    }
    { // success
        proto::Catalog inputProto = createSimpleCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.create(inputProto).code());
        auto expectedProto = inputProto;
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        expectedProto.set_version(kToUpdateCatalogVersion);
        proto::Catalog outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(CatalogTest, testUpdate) {
    { // success
        Catalog entity;
        proto::Catalog inputProto = createSimpleCatalogProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::Catalog expectedProto = createSimpleCatalogProto();
        expectedProto.mutable_catalog_config()->set_version(11);
        auto request = expectedProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);

        ASSERT_EQ(Status::OK, entity.update(request).code());
        ASSERT_EQ(proto::EntityStatus::PENDING_PUBLISH, entity.status().code());
        ASSERT_EQ(kToUpdateCatalogVersion, entity.version());
        proto::Catalog outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
    { // no acutal update
        Catalog entity;
        proto::Catalog inputProto = createSimpleCatalogProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::Catalog request = createSimpleCatalogProto();
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.update(request).code());
    }
}

TEST_F(CatalogTest, testDrop) {
    Catalog entity;
    proto::Catalog inputProto = createSimpleCatalogProto();
    ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
    ASSERT_EQ(Status::OK, entity.drop().code());
    ASSERT_EQ(proto::EntityStatus::PENDING_DELETE, entity.status().code());
    ASSERT_EQ(kToUpdateCatalogVersion, entity.version());
}

TEST_F(CatalogTest, testCreateDatabase) {
    {
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        auto request = inputProto.databases(0);
        ASSERT_EQ(Status::DUPLICATE_ENTITY, entity.createDatabase(request).code());
    }
    {
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        auto request = inputProto.databases(0);
        request.set_database_name("db2");
        ASSERT_EQ(Status::OK, entity.createDatabase(request).code());
        proto::TableGroup subRequest;
        subRequest.set_catalog_name("ct1");
        subRequest.set_database_name("db2");
        subRequest.set_table_group_name("default");
        ASSERT_EQ(Status::OK, entity.createTableGroup(subRequest).code());

        proto::Catalog expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.add_databases();
            subProto->CopyFrom(request);
            addDefaultTableGroup(*subProto);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Catalog outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

TEST_F(CatalogTest, testUpdateDatabase) {
    { // failure, database not found
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        auto request = inputProto.databases(0);
        request.set_database_name("db2");
        ASSERT_EQ(Status::NOT_FOUND, entity.updateDatabase(request).code());
    }
    { // failure, no actual change
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        auto request = inputProto.databases(0);
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.updateDatabase(request).code());
    }
    { // success
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        auto request = inputProto.databases(0);
        request.mutable_database_config()->set_version(222);
        ASSERT_EQ(Status::OK, entity.updateDatabase(request).code());

        proto::Catalog expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_databases(0);
            subProto->CopyFrom(request);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Catalog outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

TEST_F(CatalogTest, testDropDatabase) {
    { // failure, database not found
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        DatabaseId request{"unknwon", "ct1"};
        ASSERT_EQ(Status::NOT_FOUND, entity.dropDatabase(request).code());
    }
    { // success
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        DatabaseId request{"db1", "ct1"};
        ASSERT_EQ(Status::OK, entity.dropDatabase(request).code());

        proto::Catalog expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        expectedProto.clear_databases();
        proto::Catalog outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

TEST_F(CatalogTest, testGetDatabase) {
    Catalog entity;
    proto::Catalog inputProto = createFullCatalogProto();
    ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
    Database *subEntity = nullptr;
    ASSERT_EQ(Status::NOT_FOUND, entity.getDatabase("unknown", subEntity).code());
    ASSERT_EQ(nullptr, subEntity);
    ASSERT_EQ(Status::OK, entity.getDatabase("db1", subEntity).code());
    ASSERT_NE(nullptr, subEntity);
}

TEST_F(CatalogTest, testListDatabase) {
    Catalog entity;
    ASSERT_EQ(std::vector<std::string>{}, entity.listDatabase());
    proto::Catalog inputProto = createFullCatalogProto();
    ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
    ASSERT_EQ(std::vector<std::string>{"db1"}, entity.listDatabase());
}

TEST_F(CatalogTest, testCreateTable) {
    { // database not found
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::Table request;
        request.set_table_name("tb1");
        request.set_database_name("unknown");
        request.set_catalog_name("ct1");
        ASSERT_EQ(Status::NOT_FOUND, entity.createTable(request).code());
    }
    { // add table failure
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::Table request;
        request.set_table_name("tb1");
        request.set_database_name("db1");
        request.set_catalog_name("ct1");
        ASSERT_NE(Status::OK, entity.createTable(request).code());
    }
    { // success
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::Table request;
        fillTableProto(request);
        auto status = entity.createTable(request);
        ASSERT_EQ(Status::OK, status.code()) << status.message();

        proto::Catalog expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_databases(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->add_tables();
            subProto->CopyFrom(request);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->mutable_tables(0)->mutable_table_structure();
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Catalog outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

TEST_F(CatalogTest, testDropTable) {
    { // database not found
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        TableId request{"tb1", "unknown", "ct1"};
        ASSERT_EQ(Status::NOT_FOUND, entity.dropTable(request).code());
    }
    { // drop failure
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        TableId request{"tb1", "db1", "ct1"};
        ASSERT_NE(Status::OK, entity.dropTable(request).code());
    }
    { // success
        proto::Catalog inputProto = createFullCatalogProto();
        {
            auto subProto = inputProto.mutable_databases(0)->add_tables();
            fillTableProto(*subProto);
        }
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        TableId request{"tb1", "db1", "ct1"};
        auto status = entity.dropTable(request);
        ASSERT_EQ(Status::OK, status.code()) << status.message();

        proto::Catalog expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_databases(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        expectedProto.mutable_databases(0)->clear_tables();
        proto::Catalog outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

TEST_F(CatalogTest, testUpdateTable) {
    { // database not found
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::Table request;
        fillTableProto(request);
        request.set_database_name("unknown");
        ASSERT_EQ(Status::NOT_FOUND, entity.updateTable(request).code());
    }
    { // update failure
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::Table request;
        fillTableProto(request);
        ASSERT_NE(Status::OK, entity.updateTable(request).code());
    }
    { // success
        proto::Catalog inputProto = createFullCatalogProto();
        {
            auto subProto = inputProto.mutable_databases(0)->add_tables();
            fillTableProto(*subProto);
        }
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::Table request = inputProto.databases(0).tables(0);
        request.mutable_table_config()->set_version(333);
        ASSERT_EQ(Status::OK, entity.updateTable(request).code());

        proto::Catalog expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_databases(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->mutable_tables(0);
            subProto->CopyFrom(request);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Catalog outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

TEST_F(CatalogTest, testUpdateTableStructure) {
    { // database not found
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::TableStructure request;
        fillTableStructureProto(request);
        request.set_database_name("unknown");
        ASSERT_EQ(Status::NOT_FOUND, entity.updateTableStructure(request).code());
    }
    { // update failure
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::TableStructure request;
        fillTableStructureProto(request);
        ASSERT_NE(Status::OK, entity.updateTableStructure(request).code());
    }
    { // success
        proto::Catalog inputProto = createFullCatalogProto();
        {
            auto subProto = inputProto.mutable_databases(0)->add_tables();
            fillTableProto(*subProto);
        }
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::TableStructure request = inputProto.databases(0).tables(0).table_structure();
        request.mutable_table_structure_config()->set_comment("xxx");
        ASSERT_EQ(Status::OK, entity.updateTableStructure(request).code());

        proto::Catalog expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_databases(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->mutable_tables(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->mutable_tables(0)->mutable_table_structure();
            subProto->CopyFrom(request);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Catalog outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

TEST_F(CatalogTest, testAddColumn) {
    { // database not found
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::AddColumnRequest request;
        request.set_table_name("tb1");
        request.set_database_name("unknown");
        request.set_catalog_name("ct1");
        ASSERT_EQ(Status::NOT_FOUND, entity.addColumn(request).code());
    }
    { // failure
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::AddColumnRequest request;
        request.set_table_name("tb1");
        request.set_database_name("db1");
        request.set_catalog_name("ct1");
        ASSERT_NE(Status::OK, entity.addColumn(request).code());
    }
    { // success
        proto::Catalog inputProto = createFullCatalogProto();
        {
            auto subProto = inputProto.mutable_databases(0)->add_tables();
            fillTableProto(*subProto);
        }
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::AddColumnRequest request;
        request.set_table_name("tb1");
        request.set_database_name("db1");
        request.set_catalog_name("ct1");
        request.add_columns()->set_name("f1");
        ASSERT_EQ(Status::OK, entity.addColumn(request).code());

        proto::Catalog expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_databases(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->mutable_tables(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->mutable_tables(0)->mutable_table_structure();
            subProto->add_columns()->CopyFrom(request.columns(0));
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Catalog outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

TEST_F(CatalogTest, testUpdateColumn) {
    { // database not found
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::UpdateColumnRequest request;
        request.set_table_name("tb1");
        request.set_database_name("unknown");
        request.set_catalog_name("ct1");
        ASSERT_EQ(Status::NOT_FOUND, entity.updateColumn(request).code());
    }
    { // failure
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::UpdateColumnRequest request;
        request.set_table_name("tb1");
        request.set_database_name("db1");
        request.set_catalog_name("ct1");
        ASSERT_NE(Status::OK, entity.updateColumn(request).code());
    }
    { // success
        proto::Catalog inputProto = createFullCatalogProto();
        {
            auto subProto = inputProto.mutable_databases(0)->add_tables();
            fillTableProto(*subProto);
            subProto->mutable_table_structure()->add_columns()->set_name("f1");
        }
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::UpdateColumnRequest request;
        request.set_table_name("tb1");
        request.set_database_name("db1");
        request.set_catalog_name("ct1");
        {
            auto subProto = request.mutable_column();
            subProto->set_name("f1");
            subProto->set_default_value("123");
        }
        ASSERT_EQ(Status::OK, entity.updateColumn(request).code());

        proto::Catalog expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_databases(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->mutable_tables(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->mutable_tables(0)->mutable_table_structure();
            subProto->mutable_columns(0)->CopyFrom(request.column());
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Catalog outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

TEST_F(CatalogTest, testDropColumn) {
    { // database not found
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::DropColumnRequest request;
        request.set_table_name("tb1");
        request.set_database_name("unknown");
        request.set_catalog_name("ct1");
        ASSERT_EQ(Status::NOT_FOUND, entity.dropColumn(request).code());
    }
    { // failure
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::DropColumnRequest request;
        request.set_table_name("tb1");
        request.set_database_name("db1");
        request.set_catalog_name("ct1");
        ASSERT_NE(Status::OK, entity.dropColumn(request).code());
    }
    { // success
        proto::Catalog inputProto = createFullCatalogProto();
        {
            auto subProto = inputProto.mutable_databases(0)->add_tables();
            fillTableProto(*subProto);
            subProto->mutable_table_structure()->add_columns()->set_name("f1");
        }
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::DropColumnRequest request;
        request.set_table_name("tb1");
        request.set_database_name("db1");
        request.set_catalog_name("ct1");
        request.add_column_names("f1");
        ASSERT_EQ(Status::OK, entity.dropColumn(request).code());

        proto::Catalog expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_databases(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->mutable_tables(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->mutable_tables(0)->mutable_table_structure();
            subProto->clear_columns();
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Catalog outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

TEST_F(CatalogTest, testCreateIndex) {
    { // database not found
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::CreateIndexRequest request;
        request.set_table_name("tb1");
        request.set_database_name("unknown");
        request.set_catalog_name("ct1");
        ASSERT_EQ(Status::NOT_FOUND, entity.createIndex(request).code());
    }
    { // failure
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::CreateIndexRequest request;
        request.set_table_name("tb1");
        request.set_database_name("db1");
        request.set_catalog_name("ct1");
        ASSERT_NE(Status::OK, entity.createIndex(request).code());
    }
    { // success
        proto::Catalog inputProto = createFullCatalogProto();
        {
            auto subProto = inputProto.mutable_databases(0)->add_tables();
            fillTableProto(*subProto);
        }
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::CreateIndexRequest request;
        request.set_table_name("tb1");
        request.set_database_name("db1");
        request.set_catalog_name("ct1");
        request.add_indexes()->set_name("f1");
        ASSERT_EQ(Status::OK, entity.createIndex(request).code());

        proto::Catalog expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_databases(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->mutable_tables(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->mutable_tables(0)->mutable_table_structure();
            subProto->add_indexes()->CopyFrom(request.indexes(0));
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Catalog outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

TEST_F(CatalogTest, testUpdateIndex) {
    { // database not found
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::UpdateIndexRequest request;
        request.set_table_name("tb1");
        request.set_database_name("unknown");
        request.set_catalog_name("ct1");
        ASSERT_EQ(Status::NOT_FOUND, entity.updateIndex(request).code());
    }
    { // failure
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::UpdateIndexRequest request;
        request.set_table_name("tb1");
        request.set_database_name("db1");
        request.set_catalog_name("ct1");
        ASSERT_NE(Status::OK, entity.updateIndex(request).code());
    }
    { // success
        proto::Catalog inputProto = createFullCatalogProto();
        {
            auto subProto = inputProto.mutable_databases(0)->add_tables();
            fillTableProto(*subProto);
            subProto->mutable_table_structure()->add_indexes()->set_name("f1");
        }
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::UpdateIndexRequest request;
        request.set_table_name("tb1");
        request.set_database_name("db1");
        request.set_catalog_name("ct1");
        {
            auto subProto = request.mutable_index();
            subProto->set_name("f1");
            subProto->set_index_type(proto::TableStructure::Index::NUMBER);
        }
        ASSERT_EQ(Status::OK, entity.updateIndex(request).code());

        proto::Catalog expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_databases(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->mutable_tables(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->mutable_tables(0)->mutable_table_structure();
            subProto->mutable_indexes(0)->CopyFrom(request.index());
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Catalog outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

TEST_F(CatalogTest, testDropIndex) {
    { // database not found
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::DropIndexRequest request;
        request.set_table_name("tb1");
        request.set_database_name("unknown");
        request.set_catalog_name("ct1");
        ASSERT_EQ(Status::NOT_FOUND, entity.dropIndex(request).code());
    }
    { // failure
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::DropIndexRequest request;
        request.set_table_name("tb1");
        request.set_database_name("db1");
        request.set_catalog_name("ct1");
        ASSERT_NE(Status::OK, entity.dropIndex(request).code());
    }
    { // success
        proto::Catalog inputProto = createFullCatalogProto();
        {
            auto subProto = inputProto.mutable_databases(0)->add_tables();
            fillTableProto(*subProto);
            subProto->mutable_table_structure()->add_indexes()->set_name("f1");
        }
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::DropIndexRequest request;
        request.set_table_name("tb1");
        request.set_database_name("db1");
        request.set_catalog_name("ct1");
        request.add_index_names("f1");
        ASSERT_EQ(Status::OK, entity.dropIndex(request).code());

        proto::Catalog expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_databases(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->mutable_tables(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->mutable_tables(0)->mutable_table_structure();
            subProto->clear_indexes();
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Catalog outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

TEST_F(CatalogTest, testCreatePartition) {
    { // database not found
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::CreatePartitionRequest request;
        {
            auto subProto = request.mutable_partition();
            fillPartitonProto(*subProto, proto::PartitionType::TABLE_PARTITION);
            subProto->set_database_name("unknown");
        }
        ASSERT_EQ(Status::NOT_FOUND, entity.createPartition(request).code());
    }
    { // add failure
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::CreatePartitionRequest request;
        {
            auto subProto = request.mutable_partition();
            fillPartitonProto(*subProto, proto::PartitionType::TABLE_PARTITION);
        }
        ASSERT_NE(Status::OK, entity.createPartition(request).code());
    }
    { // success
        proto::Catalog inputProto = createFullCatalogProto();
        {
            auto subProto = inputProto.mutable_databases(0)->add_tables();
            fillTableProto(*subProto);
        }
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::CreatePartitionRequest request;
        {
            auto subProto = request.mutable_partition();
            fillPartitonProto(*subProto, proto::PartitionType::TABLE_PARTITION);
        }
        auto status = entity.createPartition(request);
        ASSERT_EQ(Status::OK, status.code()) << status.message();

        proto::Catalog expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_databases(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->mutable_tables(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->mutable_tables(0)->add_partitions();
            subProto->CopyFrom(request.partition());
            subProto->mutable_table_structure()->CopyFrom(inputProto.databases(0).tables(0).table_structure());
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Catalog outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

TEST_F(CatalogTest, testUpdatePartition) {
    { // database not found
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::UpdatePartitionRequest request;
        {
            auto subProto = request.mutable_partition();
            fillPartitonProto(*subProto, proto::PartitionType::TABLE_PARTITION);
            subProto->set_database_name("unknown");
        }
        ASSERT_EQ(Status::NOT_FOUND, entity.updatePartition(request).code());
    }
    { // update failure
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::UpdatePartitionRequest request;
        {
            auto subProto = request.mutable_partition();
            fillPartitonProto(*subProto, proto::PartitionType::TABLE_PARTITION);
        }
        ASSERT_NE(Status::OK, entity.updatePartition(request).code());
    }
    { // success
        proto::Catalog inputProto = createFullCatalogProto();
        {
            auto subProto = inputProto.mutable_databases(0)->add_tables();
            fillTableProto(*subProto);
        }
        {
            auto subProto = inputProto.mutable_databases(0)->mutable_tables(0)->add_partitions();
            fillPartitonProto(*subProto, proto::PartitionType::TABLE_PARTITION);
        }
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::UpdatePartitionRequest request;
        {
            auto subProto = request.mutable_partition();
            subProto->CopyFrom(inputProto.databases(0).tables(0).partitions(0));
            subProto->mutable_partition_config()->set_version(333);
        }
        ASSERT_EQ(Status::OK, entity.updatePartition(request).code());

        proto::Catalog expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_databases(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->mutable_tables(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->mutable_tables(0)->mutable_partitions(0);
            subProto->CopyFrom(request.partition());
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Catalog outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

TEST_F(CatalogTest, testUpdatePartitionTableStructure) {
    { // failure: table not found
        Catalog entity;
        proto::Catalog inputProto = createFullCatalogProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::UpdatePartitionTableStructureRequest request;
        request.set_partition_name("part1");
        request.set_table_name("tb1");
        request.set_database_name("unknown");
        ASSERT_EQ(Status::NOT_FOUND, entity.updatePartitionTableStructure(request).code());
    }
    { // failure: partition not found
        Catalog entity;
        proto::Catalog inputProto = createFullCatalogProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::UpdatePartitionTableStructureRequest request;
        request.set_partition_name("part1");
        request.set_table_name("tb1");
        request.set_database_name("db1");
        ASSERT_NE(Status::OK, entity.updatePartitionTableStructure(request).code());
    }
    { // success
        Catalog entity;
        proto::Catalog inputProto = createFullCatalogProto();
        {
            auto subProto = inputProto.mutable_databases(0)->add_tables();
            fillTableProto(*subProto);
        }
        {
            auto subProto = inputProto.mutable_databases(0)->mutable_tables(0)->add_partitions();
            fillPartitonProto(*subProto, proto::PartitionType::TABLE_PARTITION);
            subProto->mutable_table_structure()->CopyFrom(inputProto.databases(0).tables(0).table_structure());
        }
        inputProto.mutable_databases(0)->mutable_tables(0)->mutable_table_structure()->add_columns()->set_name("f1");
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::UpdatePartitionTableStructureRequest request;
        request.set_partition_name("part1");
        request.set_table_name("tb1");
        request.set_database_name("db1");
        request.set_catalog_name("ct1");
        auto status = entity.updatePartitionTableStructure(request);
        ASSERT_EQ(Status::OK, status.code()) << status.message();

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_databases(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->mutable_tables(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->mutable_tables(0)->mutable_partitions(0);
            subProto->mutable_table_structure()->CopyFrom(expectedProto.databases(0).tables(0).table_structure());
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Catalog outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(CatalogTest, testDropPartition) {
    { // database not found
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::DropPartitionRequest request;
        request.set_partition_name("part1");
        request.set_table_name("tb1");
        request.set_database_name("unknown");
        request.set_catalog_name("ct1");
        ASSERT_EQ(Status::NOT_FOUND, entity.dropPartition(request).code());
    }
    { // drop failure
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::DropPartitionRequest request;
        request.set_partition_name("part1");
        request.set_table_name("tb1");
        request.set_database_name("db1");
        request.set_catalog_name("ct1");
        ASSERT_NE(Status::OK, entity.dropPartition(request).code());
    }
    { // success
        proto::Catalog inputProto = createFullCatalogProto();
        {
            auto subProto = inputProto.mutable_databases(0)->add_tables();
            fillTableProto(*subProto);
        }
        {
            auto subProto = inputProto.mutable_databases(0)->mutable_tables(0)->add_partitions();
            fillPartitonProto(*subProto, proto::PartitionType::TABLE_PARTITION);
        }
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::DropPartitionRequest request;
        request.set_partition_name("part1");
        request.set_table_name("tb1");
        request.set_database_name("db1");
        request.set_catalog_name("ct1");
        request.set_partition_type(proto::PartitionType::TABLE_PARTITION);
        ASSERT_EQ(Status::OK, entity.dropPartition(request).code());

        proto::Catalog expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_databases(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->mutable_tables(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            subProto->clear_partitions();
        }
        proto::Catalog outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

TEST_F(CatalogTest, testCreateTableGroup) {
    { // database not found
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::TableGroup request;
        fillTableGroupProto(request);
        request.set_database_name("unknown");
        ASSERT_EQ(Status::NOT_FOUND, entity.createTableGroup(request).code());
    }
    { // add table_group failure
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::TableGroup request;
        fillTableGroupProto(request);
        request.add_load_strategies()->set_table_name("unknown");
        ASSERT_NE(Status::OK, entity.createTableGroup(request).code());
    }
    { // success
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::TableGroup request;
        fillTableGroupProto(request);
        ASSERT_EQ(Status::OK, entity.createTableGroup(request).code());

        proto::Catalog expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_databases(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->add_table_groups();
            subProto->CopyFrom(request);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Catalog outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

TEST_F(CatalogTest, testUpdateTableGroup) {
    { // database not found
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::TableGroup request;
        fillTableGroupProto(request);
        request.set_database_name("unknown");
        ASSERT_EQ(Status::NOT_FOUND, entity.updateTableGroup(request).code());
    }
    { // update failure
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::TableGroup request;
        fillTableGroupProto(request);
        ASSERT_NE(Status::OK, entity.updateTableGroup(request).code());
    }
    { // success
        proto::Catalog inputProto = createFullCatalogProto();
        {
            auto subProto = inputProto.mutable_databases(0)->add_table_groups();
            fillTableGroupProto(*subProto);
        }
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::TableGroup request = inputProto.databases(0).table_groups(0);
        request.mutable_table_group_config()->set_version(333);
        ASSERT_EQ(Status::OK, entity.updateTableGroup(request).code());

        proto::Catalog expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_databases(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->mutable_table_groups(0);
            subProto->CopyFrom(request);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Catalog outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

TEST_F(CatalogTest, testDropTableGroup) {
    { // database not found
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        TableGroupId request{"tg1", "unknown", "ct1"};
        ASSERT_EQ(Status::NOT_FOUND, entity.dropTableGroup(request).code());
    }
    { // drop failure
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        TableGroupId request{"tg1", "db1", "ct1"};
        ASSERT_NE(Status::OK, entity.dropTableGroup(request).code());
    }
    { // success
        proto::Catalog inputProto = createFullCatalogProto();
        {
            auto subProto = inputProto.mutable_databases(0)->add_table_groups();
            fillTableGroupProto(*subProto);
        }
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        TableGroupId request{"tg1", "db1", "ct1"};
        auto status = entity.dropTableGroup(request);
        ASSERT_EQ(Status::OK, status.code()) << status.message();

        proto::Catalog expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_databases(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        expectedProto.mutable_databases(0)->clear_table_groups();
        proto::Catalog outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

TEST_F(CatalogTest, testCreateLoadStrategy) {
    { // database not found
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::LoadStrategy request;
        fillLoadStrategyProto(request);
        request.set_database_name("unknown");
        ASSERT_EQ(Status::NOT_FOUND, entity.createLoadStrategy(request).code());
    }
    { // add failure
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::LoadStrategy request;
        fillLoadStrategyProto(request);
        ASSERT_NE(Status::OK, entity.createLoadStrategy(request).code());
    }
    { // success
        proto::Catalog inputProto = createFullCatalogProto();
        {
            auto subProto = inputProto.mutable_databases(0)->add_tables();
            fillTableProto(*subProto);
        }
        {
            auto subProto = inputProto.mutable_databases(0)->add_table_groups();
            fillTableGroupProto(*subProto);
        }
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::LoadStrategy request;
        fillLoadStrategyProto(request);
        ASSERT_EQ(Status::OK, entity.createLoadStrategy(request).code());

        proto::Catalog expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_databases(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->mutable_table_groups(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->mutable_table_groups(0)->add_load_strategies();
            subProto->CopyFrom(request);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Catalog outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

TEST_F(CatalogTest, testUpdateLoadStrategy) {
    { // database not found
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::LoadStrategy request;
        fillLoadStrategyProto(request);
        request.set_database_name("unknown");
        ASSERT_EQ(Status::NOT_FOUND, entity.updateLoadStrategy(request).code());
    }
    { // update failure
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::LoadStrategy request;
        fillLoadStrategyProto(request);
        ASSERT_NE(Status::OK, entity.updateLoadStrategy(request).code());
    }
    { // success
        proto::Catalog inputProto = createFullCatalogProto();
        {
            auto subProto = inputProto.mutable_databases(0)->add_tables();
            fillTableProto(*subProto);
        }
        {
            auto subProto = inputProto.mutable_databases(0)->add_table_groups();
            fillTableGroupProto(*subProto);
        }
        {
            auto subProto = inputProto.mutable_databases(0)->mutable_table_groups(0)->add_load_strategies();
            fillLoadStrategyProto(*subProto);
        }
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::LoadStrategy request;
        fillLoadStrategyProto(request);
        request.mutable_load_strategy_config()->set_version(333);
        ASSERT_EQ(Status::OK, entity.updateLoadStrategy(request).code());

        proto::Catalog expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_databases(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->mutable_table_groups(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->mutable_table_groups(0)->mutable_load_strategies(0);
            subProto->CopyFrom(request);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Catalog outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

TEST_F(CatalogTest, testDropLoadStrategy) {
    { // database not found
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        LoadStrategyId request{"tb1", "tg1", "unknown", "ct1"};
        ASSERT_EQ(Status::NOT_FOUND, entity.dropLoadStrategy(request).code());
    }
    { // drop failure
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        LoadStrategyId request{"tb1", "tg1", "db1", "ct1"};
        ASSERT_NE(Status::OK, entity.dropLoadStrategy(request).code());
    }
    { // success
        proto::Catalog inputProto = createFullCatalogProto();
        {
            auto subProto = inputProto.mutable_databases(0)->add_tables();
            fillTableProto(*subProto);
        }
        {
            auto subProto = inputProto.mutable_databases(0)->add_table_groups();
            fillTableGroupProto(*subProto);
        }
        {
            auto subProto = inputProto.mutable_databases(0)->mutable_table_groups(0)->add_load_strategies();
            fillLoadStrategyProto(*subProto);
        }
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        LoadStrategyId request{"tb1", "tg1", "db1", "ct1"};
        ASSERT_EQ(Status::OK, entity.dropLoadStrategy(request).code());

        proto::Catalog expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_databases(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->mutable_table_groups(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            subProto->clear_load_strategies();
        }
        proto::Catalog outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

TEST_F(CatalogTest, testCreateFunction) {
    { // database not found
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::Function request;
        fillFunctionProto(request);
        request.set_database_name("unknown");
        ASSERT_EQ(Status::NOT_FOUND, entity.createFunction(request).code());
    }
    { // add function failure
        proto::Catalog inputProto = createFullCatalogProto();
        {
            auto subProto = inputProto.mutable_databases(0)->add_functions();
            fillFunctionProto(*subProto);
        }
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::Function request;
        fillFunctionProto(request);
        ASSERT_NE(Status::OK, entity.createFunction(request).code());
    }
    { // success
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::Function request;
        fillFunctionProto(request);
        ASSERT_EQ(Status::OK, entity.createFunction(request).code());

        proto::Catalog expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_databases(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->add_functions();
            subProto->CopyFrom(request);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Catalog outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

TEST_F(CatalogTest, testUpdateFunction) {
    { // database not found
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::Function request;
        fillFunctionProto(request);
        request.set_database_name("unknown");
        ASSERT_EQ(Status::NOT_FOUND, entity.updateFunction(request).code());
    }
    { // update failure
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::Function request;
        fillFunctionProto(request);
        ASSERT_NE(Status::OK, entity.updateFunction(request).code());
    }
    { // success
        proto::Catalog inputProto = createFullCatalogProto();
        {
            auto subProto = inputProto.mutable_databases(0)->add_functions();
            fillFunctionProto(*subProto);
        }
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::Function request = inputProto.databases(0).functions(0);
        request.mutable_function_config()->set_version(333);
        ASSERT_EQ(Status::OK, entity.updateFunction(request).code());

        proto::Catalog expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_databases(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        {
            auto subProto = expectedProto.mutable_databases(0)->mutable_functions(0);
            subProto->CopyFrom(request);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::Catalog outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

TEST_F(CatalogTest, testDropFunction) {
    { // database not found
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        FunctionId request{"func1", "unknown", "ct1"};
        ASSERT_EQ(Status::NOT_FOUND, entity.dropFunction(request).code());
    }
    { // drop failure
        proto::Catalog inputProto = createFullCatalogProto();
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        FunctionId request{"func1", "db1", "ct1"};
        ASSERT_NE(Status::OK, entity.dropFunction(request).code());
    }
    { // success
        proto::Catalog inputProto = createFullCatalogProto();
        {
            auto subProto = inputProto.mutable_databases(0)->add_functions();
            fillFunctionProto(*subProto);
        }
        Catalog entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        FunctionId request{"func1", "db1", "ct1"};
        auto status = entity.dropFunction(request);
        ASSERT_EQ(Status::OK, status.code()) << status.message();

        proto::Catalog expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_databases(0);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        expectedProto.mutable_databases(0)->clear_functions();
        proto::Catalog outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

} // namespace catalog
