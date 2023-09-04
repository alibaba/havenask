#include "catalog/entity/TableGroup.h"

#include "gtest/gtest.h"
#include <string>
#include <vector>

#include "catalog/util/ProtoUtil.h"

namespace catalog {

class TableGroupTest : public ::testing::Test {};

auto createSimpleTableGroupProto() {
    proto::TableGroup proto;
    proto.set_table_group_name("tg1");
    proto.set_database_name("db1");
    proto.set_catalog_name("ct1");
    return proto;
}

auto createFullTableGroupProto() {
    proto::TableGroup proto = createSimpleTableGroupProto();
    proto.mutable_status()->set_code(proto::EntityStatus::PUBLISHED);
    proto.set_version(3);
    proto.mutable_table_group_config()->set_version(1);
    {
        auto subProto = proto.add_load_strategies();
        subProto->set_table_name("tb1");
        subProto->set_table_group_name("tg1");
        subProto->set_database_name("db1");
        subProto->set_catalog_name("ct1");
        subProto->mutable_load_strategy_config()->set_version(1);
        subProto->set_version(2);
    }
    proto.mutable_operation_meta()->set_created_time(123);
    return proto;
}

TEST_F(TableGroupTest, testConvert) {
    {
        proto::TableGroup inputProto;
        TableGroup entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::TableGroup inputProto;
        inputProto.set_table_group_name("tg1");
        TableGroup entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::TableGroup inputProto;
        inputProto.set_table_group_name("tg1");
        inputProto.set_database_name("db1");
        TableGroup entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::TableGroup inputProto;
        inputProto.set_table_group_name("tg1");
        inputProto.set_database_name("db1");
        inputProto.set_catalog_name("ct1");
        {
            auto subProto = inputProto.add_load_strategies();
            subProto->set_version(1);
        }
        TableGroup entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::TableGroup inputProto = createSimpleTableGroupProto();
        {
            auto subProto = inputProto.add_load_strategies();
            subProto->set_version(1);
            subProto->set_table_name("tb1");
        }
        TableGroup entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::TableGroup inputProto = createSimpleTableGroupProto();
        {
            auto subProto = inputProto.add_load_strategies();
            subProto->set_version(1);
            subProto->set_table_name("tb1");
            subProto->set_table_group_name("tg1");
        }
        TableGroup entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::TableGroup inputProto = createSimpleTableGroupProto();
        {
            auto subProto = inputProto.add_load_strategies();
            subProto->set_version(1);
            subProto->set_table_name("tb1");
            subProto->set_table_group_name("tg1");
            subProto->set_database_name("db2");
        }
        TableGroup entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::TableGroup inputProto = createSimpleTableGroupProto();
        {
            auto subProto = inputProto.add_load_strategies();
            subProto->set_version(1);
            subProto->set_table_name("tb1");
            subProto->set_table_group_name("tg1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct2");
        }
        TableGroup entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        auto inputProto = createSimpleTableGroupProto();
        TableGroup entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(proto::EntityStatus(), entity.status()));
        ASSERT_EQ(0, entity.version());
        ASSERT_EQ("tg1", entity.id().tableGroupName);
        ASSERT_EQ("db1", entity.id().databaseName);
        ASSERT_EQ("ct1", entity.id().catalogName);
        ASSERT_TRUE(ProtoUtil::compareProto(proto::TableGroupConfig(), entity.tableGroupConfig()));
        ASSERT_EQ(0UL, entity.loadStrategies().size());

        proto::TableGroup outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(inputProto, outputProto));
    }
    {
        auto inputProto = createFullTableGroupProto();
        TableGroup entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(inputProto.status(), entity.status()));
        ASSERT_EQ(3, entity.version());
        ASSERT_EQ("tg1", entity.id().tableGroupName);
        ASSERT_EQ("db1", entity.id().databaseName);
        ASSERT_EQ("ct1", entity.id().catalogName);
        ASSERT_TRUE(ProtoUtil::compareProto(inputProto.table_group_config(), entity.tableGroupConfig()));
        {
            ASSERT_EQ(1UL, entity.loadStrategies().size());
            const auto &loadStrategy = entity.loadStrategies().at("tb1");
            proto::LoadStrategy expectedProto;
            ASSERT_EQ(Status::OK, loadStrategy->toProto(&expectedProto).code());
            ASSERT_TRUE(ProtoUtil::compareProto(inputProto.load_strategies(0), expectedProto));
        }
        {
            proto::TableGroup outputProto;
            ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
            ASSERT_TRUE(ProtoUtil::compareProto(inputProto, outputProto));
        }
        {
            proto::TableGroup outputProto;
            ASSERT_EQ(Status::OK, entity.toProto(&outputProto, proto::DetailLevel::SUMMARY).code());
            auto expected = inputProto;
            {
                auto subProto = expected.mutable_load_strategies(0);
                subProto->clear_table_group_name();
                subProto->clear_database_name();
                subProto->clear_catalog_name();
                subProto->clear_load_strategy_config();
                subProto->clear_operation_meta();
            }
            ASSERT_TRUE(ProtoUtil::compareProto(expected, outputProto));
        }
        {
            proto::TableGroup outputProto;
            ASSERT_EQ(Status::OK, entity.toProto(&outputProto, proto::DetailLevel::MINIMAL).code());
            auto expected = inputProto;
            expected.clear_database_name();
            expected.clear_catalog_name();
            expected.clear_table_group_config();
            expected.clear_load_strategies();
            expected.clear_operation_meta();
            std::string diff;
            ASSERT_TRUE(ProtoUtil::compareProto(expected, outputProto, &diff)) << diff;
        }
    }
    { // duplicated loadStrategies
        auto inputProto = createSimpleTableGroupProto();
        {
            auto subProto = inputProto.add_load_strategies();
            subProto->set_table_name("tb1");
            subProto->set_table_group_name("tg1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
            subProto->mutable_load_strategy_config()->set_version(1);
        }
        {
            auto subProto = inputProto.add_load_strategies();
            subProto->set_table_name("tb1");
            subProto->set_table_group_name("tg1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
            subProto->mutable_load_strategy_config()->set_version(1);
        }
        TableGroup entity;
        ASSERT_EQ(Status::DUPLICATE_ENTITY, entity.fromProto(inputProto).code());
    }
    { // check toProto: loadStrategies sorted by name
        auto inputProto = createSimpleTableGroupProto();
        {
            auto subProto = inputProto.add_load_strategies();
            subProto->set_table_name("tb2");
            subProto->set_table_group_name("tg1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
            subProto->mutable_load_strategy_config()->set_version(1);
        }
        {
            auto subProto = inputProto.add_load_strategies();
            subProto->set_table_name("tb1");
            subProto->set_table_group_name("tg1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
            subProto->mutable_load_strategy_config()->set_version(1);
        }
        TableGroup entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_EQ(2UL, entity.loadStrategies().size());

        proto::TableGroup outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        auto expectedProto = createSimpleTableGroupProto();
        expectedProto.add_load_strategies()->CopyFrom(inputProto.load_strategies(1));
        expectedProto.add_load_strategies()->CopyFrom(inputProto.load_strategies(0));
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(TableGroupTest, testClone) {
    auto inputProto = createFullTableGroupProto();

    TableGroup entity;
    ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

    auto newEntity = entity.clone();
    ASSERT_NE(nullptr, newEntity);

    proto::TableGroup outputProto;
    ASSERT_EQ(Status::OK, newEntity->toProto(&outputProto).code());
    ASSERT_TRUE(ProtoUtil::compareProto(inputProto, outputProto));
}

TEST_F(TableGroupTest, testCompare) {
    { // no diff
        auto inputProto1 = createFullTableGroupProto();
        TableGroup entity1;
        ASSERT_EQ(Status::OK, entity1.fromProto(inputProto1).code());

        auto inputProto2 = createFullTableGroupProto();
        TableGroup entity2;
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
    { // diff with nullptr
        auto inputProto2 = createFullTableGroupProto();
        inputProto2.set_version(10);
        inputProto2.mutable_table_group_config()->set_version(11);
        TableGroup entity2;
        ASSERT_EQ(Status::OK, entity2.fromProto(inputProto2).code());

        DiffResult diffResult;
        ASSERT_EQ(Status::OK, entity2.compare(nullptr, diffResult).code());
        ASSERT_TRUE(diffResult.hasDiff());
        ASSERT_EQ(1UL, diffResult.tableGroups.size());

        proto::TableGroupRecord expectedProto;
        expectedProto.set_table_group_name("tg1");
        expectedProto.set_database_name("db1");
        expectedProto.set_catalog_name("ct1");
        expectedProto.set_version(10);
        {
            auto subProto = expectedProto.mutable_table_group_config();
            subProto->set_version(11);
        }
        {
            auto subProto = expectedProto.add_load_strategy_records();
            subProto->set_entity_name("tb1");
            subProto->set_version(2);
        }
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PUBLISHED);
        expectedProto.mutable_operation_meta()->set_created_time(123);
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, *diffResult.tableGroups[0], &diff)) << diff;

        ASSERT_EQ(1UL, diffResult.loadStrategies.size());
        ASSERT_EQ("tb1", diffResult.loadStrategies[0]->table_name());
        ASSERT_EQ(2, diffResult.loadStrategies[0]->version());
    }
    { // diff with other
        auto inputProto1 = createFullTableGroupProto();
        TableGroup entity1;
        ASSERT_EQ(Status::OK, entity1.fromProto(inputProto1).code());

        auto inputProto2 = createFullTableGroupProto();
        inputProto2.set_version(10);
        inputProto2.mutable_table_group_config()->set_version(11);
        TableGroup entity2;
        ASSERT_EQ(Status::OK, entity2.fromProto(inputProto2).code());

        DiffResult diffResult;
        ASSERT_EQ(Status::OK, entity2.compare(&entity1, diffResult).code());
        ASSERT_TRUE(diffResult.hasDiff());
        ASSERT_EQ(1UL, diffResult.tableGroups.size());
        ASSERT_EQ(0UL, diffResult.loadStrategies.size());

        proto::TableGroupRecord expectedProto;
        expectedProto.set_table_group_name("tg1");
        expectedProto.set_database_name("db1");
        expectedProto.set_catalog_name("ct1");
        expectedProto.set_version(10);
        {
            auto subProto = expectedProto.mutable_table_group_config();
            subProto->set_version(11);
        }
        {
            auto subProto = expectedProto.add_load_strategy_records();
            subProto->set_entity_name("tb1");
            subProto->set_version(2);
        }
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PUBLISHED);
        expectedProto.mutable_operation_meta()->set_created_time(123);
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, *diffResult.tableGroups[0], &diff)) << diff;
    }
    { // diff with new load_strategy
        auto inputProto1 = createFullTableGroupProto();
        TableGroup entity1;
        ASSERT_EQ(Status::OK, entity1.fromProto(inputProto1).code());

        auto inputProto2 = createFullTableGroupProto();
        inputProto2.set_version(10);
        inputProto2.mutable_table_group_config()->set_version(11);
        {
            auto subProto = inputProto2.add_load_strategies();
            subProto->set_table_name("tb2");
            subProto->set_table_group_name("tg1");
            subProto->set_database_name("db1");
            subProto->set_catalog_name("ct1");
            subProto->set_version(3);
        }
        TableGroup entity2;
        ASSERT_EQ(Status::OK, entity2.fromProto(inputProto2).code());

        DiffResult diffResult;
        ASSERT_EQ(Status::OK, entity2.compare(&entity1, diffResult).code());
        ASSERT_TRUE(diffResult.hasDiff());
        ASSERT_EQ(1UL, diffResult.tableGroups.size());
        {
            proto::TableGroupRecord expectedProto;
            expectedProto.set_table_group_name("tg1");
            expectedProto.set_database_name("db1");
            expectedProto.set_catalog_name("ct1");
            expectedProto.set_version(10);
            {
                auto subProto = expectedProto.mutable_table_group_config();
                subProto->set_version(11);
            }
            {
                auto subProto = expectedProto.add_load_strategy_records();
                subProto->set_entity_name("tb1");
                subProto->set_version(2);
            }
            {
                auto subProto = expectedProto.add_load_strategy_records();
                subProto->set_entity_name("tb2");
                subProto->set_version(3);
            }
            expectedProto.mutable_status()->set_code(proto::EntityStatus::PUBLISHED);
            expectedProto.mutable_operation_meta()->set_created_time(123);
            std::string diff;
            ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, *diffResult.tableGroups[0], &diff)) << diff;
        }
        ASSERT_EQ(1UL, diffResult.loadStrategies.size());
        ASSERT_EQ("tb2", diffResult.loadStrategies[0]->table_name());
        ASSERT_EQ(3, diffResult.loadStrategies[0]->version());
    }
}

TEST_F(TableGroupTest, testAlignVersion) {
    {
        proto::TableGroup inputProto = createFullTableGroupProto();
        TableGroup entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_EQ(3, entity.version());
        ASSERT_EQ(2, entity.loadStrategies().at("tb1")->version());
        entity.alignVersion(20);
        ASSERT_EQ(3, entity.version());
        ASSERT_EQ(2, entity.loadStrategies().at("tb1")->version());
    }
    {
        proto::TableGroup inputProto = createFullTableGroupProto();
        inputProto.set_version(kToUpdateCatalogVersion);
        inputProto.mutable_load_strategies(0)->set_version(kToUpdateCatalogVersion);
        TableGroup entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_EQ(kToUpdateCatalogVersion, entity.version());
        ASSERT_EQ(kToUpdateCatalogVersion, entity.loadStrategies().at("tb1")->version());
        entity.alignVersion(20);
        ASSERT_EQ(20, entity.version());
        ASSERT_EQ(20, entity.loadStrategies().at("tb1")->version());
    }
}

TEST_F(TableGroupTest, testCreate) {
    { // failure: partition with table_structure
        proto::TableGroup inputProto = createFullTableGroupProto();
        inputProto.mutable_load_strategies(0)->set_table_group_name("unknown");
        TableGroup entity;
        ASSERT_NE(Status::OK, entity.create(inputProto).code());
    }
    { // simple case: no partition
        proto::TableGroup inputProto = createSimpleTableGroupProto();
        TableGroup entity;
        ASSERT_EQ(Status::OK, entity.create(inputProto).code());

        auto expectedProto = inputProto;
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        expectedProto.set_version(kToUpdateCatalogVersion);
        proto::TableGroup outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
    { // complex case: with partition
        proto::TableGroup inputProto = createFullTableGroupProto();
        TableGroup entity;
        ASSERT_EQ(Status::OK, entity.create(inputProto).code());

        auto expectedProto = inputProto;
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        expectedProto.set_version(kToUpdateCatalogVersion);
        {
            auto subProto = expectedProto.mutable_load_strategies(0);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
            subProto->set_version(kToUpdateCatalogVersion);
        }
        proto::TableGroup outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

TEST_F(TableGroupTest, testDrop) {
    auto inputProto = createFullTableGroupProto();
    TableGroup entity;
    ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
    ASSERT_NE(proto::EntityStatus::PENDING_DELETE, entity.status().code());
    ASSERT_NE(kToUpdateCatalogVersion, entity.version());
    ASSERT_EQ(Status::OK, entity.drop().code());
    ASSERT_EQ(proto::EntityStatus::PENDING_DELETE, entity.status().code());
    ASSERT_EQ(kToUpdateCatalogVersion, entity.version());
}

TEST_F(TableGroupTest, testUpdate) {
    { // success
        TableGroup entity;
        proto::TableGroup inputProto = createFullTableGroupProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::TableGroup expectedProto = createFullTableGroupProto();
        expectedProto.mutable_table_group_config()->set_version(11);
        auto request = expectedProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);

        ASSERT_EQ(Status::OK, entity.update(request).code());
        ASSERT_EQ(proto::EntityStatus::PENDING_PUBLISH, entity.status().code());
        ASSERT_EQ(kToUpdateCatalogVersion, entity.version());
        proto::TableGroup outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
    { // no acutal update
        TableGroup entity;
        proto::TableGroup inputProto = createFullTableGroupProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::TableGroup request = createFullTableGroupProto();
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.update(request).code());
    }
}

TEST_F(TableGroupTest, testCreateLoadStrategy) {
    { // failure: invalid request
        TableGroup entity;
        proto::TableGroup inputProto = createFullTableGroupProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::LoadStrategy request;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.createLoadStrategy(request).code());
    }
    { // failure: invalid request
        TableGroup entity;
        proto::TableGroup inputProto = createFullTableGroupProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::LoadStrategy request;
        request.set_table_group_name("tg2");
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.createLoadStrategy(request).code());
    }
    { // failure: invalid request
        TableGroup entity;
        proto::TableGroup inputProto = createFullTableGroupProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::LoadStrategy request;
        request.set_table_group_name("tg1");
        request.set_database_name("db2");
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.createLoadStrategy(request).code());
    }
    { // failure: invalid request
        TableGroup entity;
        proto::TableGroup inputProto = createFullTableGroupProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::LoadStrategy request;
        request.set_table_group_name("tg1");
        request.set_database_name("db1");
        request.set_catalog_name("ct2");
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.createLoadStrategy(request).code());
    }
    { // failure: load_strategy already exists
        TableGroup entity;
        proto::TableGroup inputProto = createFullTableGroupProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        auto request = inputProto.load_strategies(0);
        ASSERT_EQ(Status::DUPLICATE_ENTITY, entity.createLoadStrategy(request).code());
    }
    { // success
        TableGroup entity;
        proto::TableGroup inputProto = createFullTableGroupProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        auto request = inputProto.load_strategies(0);
        request.set_table_name("tb2");
        ASSERT_EQ(Status::OK, entity.createLoadStrategy(request).code());

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.add_load_strategies();
            subProto->CopyFrom(request);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::TableGroup outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

TEST_F(TableGroupTest, testDropLoadStrategy) {
    { // failure: partition not found
        TableGroup entity;
        proto::TableGroup inputProto = createFullTableGroupProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        LoadStrategyId request{"unknown", "tb1", "db1", "ct1"};
        ASSERT_EQ(Status::NOT_FOUND, entity.dropLoadStrategy(request).code());
    }
    { // success
        TableGroup entity;
        proto::TableGroup inputProto = createFullTableGroupProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        LoadStrategyId request{"tb1", "tb1", "db1", "ct1"};
        ASSERT_EQ(Status::OK, entity.dropLoadStrategy(request).code());

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        expectedProto.clear_load_strategies();
        proto::TableGroup outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(TableGroupTest, testUpdateLoadStrategy) {
    { // failure: load_strategy not found
        TableGroup entity;
        proto::TableGroup inputProto = createFullTableGroupProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::LoadStrategy request;
        request.set_table_name("unknown");
        ASSERT_EQ(Status::NOT_FOUND, entity.updateLoadStrategy(request).code());
    }
    { // failure: update failure
        TableGroup entity;
        proto::TableGroup inputProto = createFullTableGroupProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        proto::LoadStrategy request = inputProto.load_strategies(0);
        ASSERT_NE(Status::OK, entity.updateLoadStrategy(request).code());
    }
    { // success
        TableGroup entity;
        proto::TableGroup inputProto = createFullTableGroupProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::LoadStrategy request = inputProto.load_strategies(0);
        request.mutable_load_strategy_config()->set_version(11);
        ASSERT_EQ(Status::OK, entity.updateLoadStrategy(request).code());

        auto expectedProto = inputProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        {
            auto subProto = expectedProto.mutable_load_strategies(0);
            subProto->CopyFrom(request);
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        proto::TableGroup outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(TableGroupTest, getLoadStrategy) {
    TableGroup entity;
    proto::TableGroup inputProto = createFullTableGroupProto();
    ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
    LoadStrategy *partition = nullptr;
    ASSERT_EQ(Status::NOT_FOUND, entity.getLoadStrategy("unknown", partition).code());
    ASSERT_EQ(nullptr, partition);
    ASSERT_EQ(Status::OK, entity.getLoadStrategy("tb1", partition).code());
    ASSERT_NE(nullptr, partition);
}

TEST_F(TableGroupTest, listLoadStrategy) {
    TableGroup entity;
    ASSERT_EQ(std::vector<std::string>{}, entity.listLoadStrategy());
    proto::TableGroup inputProto = createFullTableGroupProto();
    ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
    ASSERT_EQ(std::vector<std::string>{"tb1"}, entity.listLoadStrategy());
}

} // namespace catalog
