#include "catalog/entity/Function.h"

#include "gtest/gtest.h"
#include <string>

#include "catalog/util/ProtoUtil.h"

namespace catalog {

class FunctionTest : public ::testing::Test {};

auto createFunctionProto() {
    proto::Function proto;
    proto.set_function_name("func1");
    proto.set_database_name("db1");
    proto.set_catalog_name("ct1");
    proto.set_version(5);

    proto.mutable_status()->set_code(proto::EntityStatus::PUBLISHED);
    proto.mutable_function_config()->set_version(1);
    proto.mutable_operation_meta()->set_created_time(123);
    return proto;
}

TEST_F(FunctionTest, testConvert) {
    {
        proto::Function inputProto;
        Function entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::Function inputProto;
        inputProto.set_function_name("func1");
        Function entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::Function inputProto;
        inputProto.set_function_name("func1");
        inputProto.set_database_name("db2");
        Function entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.fromProto(inputProto).code());
    }
    {
        proto::Function inputProto = createFunctionProto();

        Function entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(inputProto.status(), entity.status()));
        ASSERT_EQ(5, entity.version());
        ASSERT_EQ("func1", entity.id().functionName);
        ASSERT_EQ("db1", entity.id().databaseName);
        ASSERT_EQ("ct1", entity.id().catalogName);
        ASSERT_TRUE(ProtoUtil::compareProto(inputProto.function_config(), entity.functionConfig()));
        {
            proto::Function outputProto;
            ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
            ASSERT_TRUE(ProtoUtil::compareProto(inputProto, outputProto));
        }
        {
            proto::Function outputProto;
            ASSERT_EQ(Status::OK, entity.toProto(&outputProto, proto::DetailLevel::SUMMARY).code());
            ASSERT_TRUE(ProtoUtil::compareProto(inputProto, outputProto));
        }
        {
            proto::Function outputProto;
            ASSERT_EQ(Status::OK, entity.toProto(&outputProto, proto::DetailLevel::MINIMAL).code());
            auto expected = inputProto;
            expected.clear_database_name();
            expected.clear_catalog_name();
            expected.clear_function_config();
            expected.clear_operation_meta();
            std::string diff;
            ASSERT_TRUE(ProtoUtil::compareProto(expected, outputProto, &diff)) << diff;
        }
    }
}

TEST_F(FunctionTest, testClone) {
    proto::Function inputProto = createFunctionProto();

    Function entity;
    ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

    auto newEntity = entity.clone();
    ASSERT_NE(nullptr, newEntity);

    proto::Function outputProto;
    ASSERT_EQ(Status::OK, newEntity->toProto(&outputProto).code());
    ASSERT_TRUE(ProtoUtil::compareProto(inputProto, outputProto));
}

TEST_F(FunctionTest, testCompare) {
    {
        proto::Function inputProto = createFunctionProto();
        Function entity1;
        ASSERT_EQ(Status::OK, entity1.fromProto(inputProto).code());
        Function entity2;
        ASSERT_EQ(Status::OK, entity2.fromProto(inputProto).code());

        DiffResult diffResult;
        ASSERT_EQ(Status::OK, entity2.compare(&entity1, diffResult).code());
        ASSERT_FALSE(diffResult.hasDiff());
    }
    {
        proto::Function inputProto2 = createFunctionProto();
        inputProto2.set_version(7);
        inputProto2.mutable_function_config()->set_version(11);
        Function entity2;
        ASSERT_EQ(Status::OK, entity2.fromProto(inputProto2).code());

        DiffResult diffResult;
        ASSERT_EQ(Status::OK, entity2.compare(nullptr, diffResult).code());
        ASSERT_TRUE(diffResult.hasDiff());
        ASSERT_EQ(1UL, diffResult.functions.size());

        proto::FunctionRecord expectedProto;
        expectedProto.set_function_name("func1");
        expectedProto.set_database_name("db1");
        expectedProto.set_catalog_name("ct1");
        expectedProto.set_version(inputProto2.version());
        expectedProto.mutable_function_config()->CopyFrom(inputProto2.function_config());
        expectedProto.mutable_status()->CopyFrom(inputProto2.status());
        expectedProto.mutable_operation_meta()->set_created_time(123);
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, *diffResult.functions[0], &diff)) << diff;
    }
    {
        proto::Function inputProto1 = createFunctionProto();
        Function entity1;
        ASSERT_EQ(Status::OK, entity1.fromProto(inputProto1).code());

        proto::Function inputProto2 = createFunctionProto();
        inputProto2.set_version(7);
        inputProto2.mutable_function_config()->set_version(11);
        Function entity2;
        ASSERT_EQ(Status::OK, entity2.fromProto(inputProto2).code());

        DiffResult diffResult;
        ASSERT_EQ(Status::OK, entity2.compare(&entity1, diffResult).code());
        ASSERT_TRUE(diffResult.hasDiff());
        ASSERT_EQ(1UL, diffResult.functions.size());

        proto::FunctionRecord expectedProto;
        expectedProto.set_function_name("func1");
        expectedProto.set_database_name("db1");
        expectedProto.set_catalog_name("ct1");
        expectedProto.set_version(inputProto2.version());
        expectedProto.mutable_function_config()->CopyFrom(inputProto2.function_config());
        expectedProto.mutable_status()->CopyFrom(inputProto2.status());
        expectedProto.mutable_operation_meta()->set_created_time(123);
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, *diffResult.functions[0]));
    }
}

TEST_F(FunctionTest, testAlignVersion) {
    {
        proto::Function inputProto = createFunctionProto();
        Function entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_EQ(5, entity.version());
        entity.alignVersion(20);
        ASSERT_EQ(5, entity.version());
    }
    {
        proto::Function inputProto = createFunctionProto();
        inputProto.set_version(kToUpdateCatalogVersion);
        Function entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_EQ(kToUpdateCatalogVersion, entity.version());
        entity.alignVersion(20);
        ASSERT_EQ(20, entity.version());
    }
}

TEST_F(FunctionTest, testCreate) {
    { // failure
        proto::Function inputProto;
        Function entity;
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.create(inputProto).code());
    }
    { // success
        proto::Function inputProto = createFunctionProto();
        Function entity;
        ASSERT_EQ(Status::OK, entity.create(inputProto).code());
        auto expectedProto = inputProto;
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        expectedProto.set_version(kToUpdateCatalogVersion);
        proto::Function outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
}

TEST_F(FunctionTest, testUpdate) {
    { // success
        Function entity;
        proto::Function inputProto = createFunctionProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::Function expectedProto = createFunctionProto();
        expectedProto.mutable_function_config()->set_version(11);
        auto request = expectedProto;
        expectedProto.set_version(kToUpdateCatalogVersion);
        expectedProto.mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);

        ASSERT_EQ(Status::OK, entity.update(request).code());
        ASSERT_EQ(proto::EntityStatus::PENDING_PUBLISH, entity.status().code());
        ASSERT_EQ(kToUpdateCatalogVersion, entity.version());
        proto::Function outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto));
    }
    { // no acutal update
        Function entity;
        proto::Function inputProto = createFunctionProto();
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

        proto::Function request = createFunctionProto();
        ASSERT_EQ(Status::INVALID_ARGUMENTS, entity.update(request).code());
    }
}
TEST_F(FunctionTest, testDrop) {
    Function entity;
    proto::Function inputProto = createFunctionProto();
    ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
    ASSERT_EQ(Status::OK, entity.drop().code());
    ASSERT_EQ(proto::EntityStatus::PENDING_DELETE, entity.status().code());
    ASSERT_EQ(kToUpdateCatalogVersion, entity.version());
}

} // namespace catalog
