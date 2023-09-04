#include "catalog/entity/LoadStrategy.h"

#include "gtest/gtest.h"
#include <string>

#include "catalog/util/ProtoUtil.h"

namespace catalog {

class LoadStrategyTest : public ::testing::Test {};

auto createLoadStrategyProto() {
    proto::LoadStrategy proto;
    proto.set_table_name("tb1");
    proto.set_table_group_name("tg1");
    proto.set_database_name("db1");
    proto.set_catalog_name("ct1");
    proto.set_version(5);

    proto.mutable_status()->set_code(proto::EntityStatus::PUBLISHED);
    proto.mutable_load_strategy_config()->set_version(1);
    proto.mutable_operation_meta()->set_created_time(123);
    return proto;
}

TEST_F(LoadStrategyTest, testConvert) {
    {
        proto::LoadStrategy inputProto;
        LoadStrategy entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::LoadStrategy inputProto;
        inputProto.set_table_name("tb2");
        LoadStrategy entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::LoadStrategy inputProto;
        inputProto.set_table_name("tb1");
        inputProto.set_table_group_name("tg2");
        LoadStrategy entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::LoadStrategy inputProto;
        inputProto.set_table_name("tb1");
        inputProto.set_table_group_name("tg1");
        inputProto.set_database_name("db2");
        LoadStrategy entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::LoadStrategy inputProto = createLoadStrategyProto();

        LoadStrategy entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(inputProto.status(), entity.status()));
        ASSERT_EQ(5, entity.version());
        ASSERT_EQ("tb1", entity.id().tableName);
        ASSERT_EQ("tg1", entity.id().tableGroupName);
        ASSERT_EQ("db1", entity.id().databaseName);
        ASSERT_EQ("ct1", entity.id().catalogName);
        ASSERT_TRUE(ProtoUtil::compareProto(inputProto.load_strategy_config(), entity.loadStrategyConfig()));
        {
            proto::LoadStrategy outputProto;
            ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
            ASSERT_TRUE(ProtoUtil::compareProto(inputProto, outputProto));
        }
        {
            proto::LoadStrategy outputProto;
            ASSERT_EQ(Status::OK, entity.toProto(&outputProto, proto::DetailLevel::SUMMARY).code());
            ASSERT_TRUE(ProtoUtil::compareProto(inputProto, outputProto));
        }
        {
            proto::LoadStrategy outputProto;
            ASSERT_EQ(Status::OK, entity.toProto(&outputProto, proto::DetailLevel::MINIMAL).code());
            auto expected = inputProto;
            expected.clear_table_group_name();
            expected.clear_database_name();
            expected.clear_catalog_name();
            expected.clear_load_strategy_config();
            expected.clear_operation_meta();
            std::string diff;
            ASSERT_TRUE(ProtoUtil::compareProto(expected, outputProto, &diff)) << diff;
        }
    }
}

TEST_F(LoadStrategyTest, testClone) {
    proto::LoadStrategy inputProto = createLoadStrategyProto();

    LoadStrategy entity;
    ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

    auto newEntity = entity.clone();
    ASSERT_NE(nullptr, newEntity);

    proto::LoadStrategy outputProto;
    ASSERT_EQ(Status::OK, newEntity->toProto(&outputProto).code());
    ASSERT_TRUE(ProtoUtil::compareProto(inputProto, outputProto));
}

TEST_F(LoadStrategyTest, testCompare) {
    {
        proto::LoadStrategy inputProto = createLoadStrategyProto();
        LoadStrategy entity1;
        ASSERT_EQ(Status::OK, entity1.fromProto(inputProto).code());
        LoadStrategy entity2;
        ASSERT_EQ(Status::OK, entity2.fromProto(inputProto).code());

        DiffResult diffResult;
        ASSERT_EQ(Status::OK, entity2.compare(&entity1, diffResult).code());
        ASSERT_FALSE(diffResult.hasDiff());
    }
    {
        proto::LoadStrategy inputProto2 = createLoadStrategyProto();
        inputProto2.set_version(7);
        inputProto2.mutable_load_strategy_config()->set_version(11);
        LoadStrategy entity2;
        ASSERT_EQ(Status::OK, entity2.fromProto(inputProto2).code());

        DiffResult diffResult;
        ASSERT_EQ(Status::OK, entity2.compare(nullptr, diffResult).code());
        ASSERT_TRUE(diffResult.hasDiff());
        ASSERT_EQ(1UL, diffResult.loadStrategies.size());

        proto::LoadStrategyRecord expectedProto;
        expectedProto.set_table_name("tb1");
        expectedProto.set_table_group_name("tg1");
        expectedProto.set_database_name("db1");
        expectedProto.set_catalog_name("ct1");
        expectedProto.set_version(inputProto2.version());
        expectedProto.mutable_load_strategy_config()->CopyFrom(inputProto2.load_strategy_config());
        expectedProto.mutable_status()->CopyFrom(inputProto2.status());
        expectedProto.mutable_operation_meta()->set_created_time(123);
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, *diffResult.loadStrategies[0], &diff)) << diff;
    }
    {
        proto::LoadStrategy inputProto1 = createLoadStrategyProto();
        LoadStrategy entity1;
        ASSERT_EQ(Status::OK, entity1.fromProto(inputProto1).code());

        proto::LoadStrategy inputProto2 = createLoadStrategyProto();
        inputProto2.set_version(7);
        inputProto2.mutable_load_strategy_config()->set_version(11);
        LoadStrategy entity2;
        ASSERT_EQ(Status::OK, entity2.fromProto(inputProto2).code());

        DiffResult diffResult;
        ASSERT_EQ(Status::OK, entity2.compare(&entity1, diffResult).code());
        ASSERT_TRUE(diffResult.hasDiff());
        ASSERT_EQ(1UL, diffResult.loadStrategies.size());

        proto::LoadStrategyRecord expectedProto;
        expectedProto.set_table_name("tb1");
        expectedProto.set_table_group_name("tg1");
        expectedProto.set_database_name("db1");
        expectedProto.set_catalog_name("ct1");
        expectedProto.set_version(inputProto2.version());
        expectedProto.mutable_load_strategy_config()->CopyFrom(inputProto2.load_strategy_config());
        expectedProto.mutable_status()->CopyFrom(inputProto2.status());
        expectedProto.mutable_operation_meta()->set_created_time(123);
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, *diffResult.loadStrategies[0]));
    }
}

TEST_F(LoadStrategyTest, testAlignVersion) {
    {
        proto::LoadStrategy inputProto = createLoadStrategyProto();
        LoadStrategy entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_EQ(5, entity.version());
        entity.alignVersion(20);
        ASSERT_EQ(5, entity.version());
    }
    {
        proto::LoadStrategy inputProto = createLoadStrategyProto();
        inputProto.set_version(kToUpdateCatalogVersion);
        LoadStrategy entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_EQ(kToUpdateCatalogVersion, entity.version());
        entity.alignVersion(20);
        ASSERT_EQ(20, entity.version());
    }
}

TEST_F(LoadStrategyTest, testCreate) {
    { // failure
        proto::LoadStrategy inputProto;
        LoadStrategy entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.create(inputProto).code());
    }
    { // success
        proto::LoadStrategy inputProto = createLoadStrategyProto();
        LoadStrategy entity;
        ASSERT_EQ(Status::OK, entity.create(inputProto).code());
        auto expectedProto = inputProto;
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        expectedProto.set_version(kToUpdateCatalogVersion);
        proto::LoadStrategy outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(LoadStrategyTest, testUpdate) {
    { // success
        LoadStrategy entity;
        proto::LoadStrategy inputProto = createLoadStrategyProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::LoadStrategy expectedProto = createLoadStrategyProto();
        expectedProto.mutable_load_strategy_config()->set_version(11);
        auto request = expectedProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);

        ASSERT_EQ(Status::OK, entity.update(request).code());
        ASSERT_EQ(proto::EntityStatus::PENDING_PUBLISH, entity.status().code());
        ASSERT_EQ(kToUpdateCatalogVersion, entity.version());
        proto::LoadStrategy outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
    { // no acutal update
        LoadStrategy entity;
        proto::LoadStrategy inputProto = createLoadStrategyProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::LoadStrategy request = createLoadStrategyProto();
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.update(request).code());
    }
}
TEST_F(LoadStrategyTest, testDrop) {
    LoadStrategy entity;
    proto::LoadStrategy inputProto = createLoadStrategyProto();
    ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
    ASSERT_EQ(Status::OK, entity.drop().code());
    ASSERT_EQ(proto::EntityStatus::PENDING_DELETE, entity.status().code());
    ASSERT_EQ(kToUpdateCatalogVersion, entity.version());
}

} // namespace catalog
