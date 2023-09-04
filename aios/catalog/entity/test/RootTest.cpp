#include "catalog/entity/Root.h"

#include "gtest/gtest.h"
#include <string>

#include "catalog/util/ProtoUtil.h"

namespace catalog {

class RootTest : public ::testing::Test {};

TEST_F(RootTest, testConvert) {
    {
        proto::Root inputProto;
        Root entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_EQ(0, entity.version());
        ASSERT_EQ("", entity.rootName());
    }
    {
        proto::Root inputProto;
        inputProto.set_root_name("test");
        Root entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_EQ(0, entity.version());
        ASSERT_EQ("test", entity.rootName());
        ASSERT_EQ(std::unordered_set<std::string>(), entity.catalogNames());

        proto::Root outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        ASSERT_TRUE(ProtoUtil::compareProto(outputProto, inputProto));
    }
    { // duplicated entity
        proto::Root inputProto;
        inputProto.set_root_name("test");
        inputProto.set_version(3);
        inputProto.add_catalog_names("ct1");
        inputProto.add_catalog_names("ct1");

        Root entity;
        auto status = entity.fromProto(inputProto);
        ASSERT_EQ(Status::DUPLICATE_ENTITY, status.code()) << status.message();
    }
    {
        proto::Root inputProto;
        inputProto.set_root_name("test");
        inputProto.set_version(3);
        inputProto.add_catalog_names("ct2");
        inputProto.add_catalog_names("ct1");
        inputProto.add_catalog_names("ct3");

        Root entity;
        ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());
        ASSERT_EQ(3, entity.version());
        ASSERT_EQ("test", entity.rootName());
        ASSERT_EQ(std::unordered_set<std::string>({"ct1", "ct3", "ct2"}), entity.catalogNames());

        proto::Root expectedProto = inputProto;
        expectedProto.clear_catalog_names();
        expectedProto.add_catalog_names("ct1");
        expectedProto.add_catalog_names("ct2");
        expectedProto.add_catalog_names("ct3");
        proto::Root outputProto;
        ASSERT_EQ(Status::OK, entity.toProto(&outputProto).code());
        std::string diff;
        ASSERT_TRUE(ProtoUtil::compareProto(expectedProto, outputProto, &diff)) << diff;
    }
}

TEST_F(RootTest, testClone) {
    proto::Root inputProto;
    inputProto.set_root_name("test");
    inputProto.set_version(3);
    inputProto.add_catalog_names("ct1");
    inputProto.add_catalog_names("ct2");

    Root entity;
    ASSERT_EQ(Status::OK, entity.fromProto(inputProto).code());

    auto newEntity = entity.clone();
    ASSERT_NE(nullptr, newEntity);

    proto::Root outputProto;
    ASSERT_EQ(Status::OK, newEntity->toProto(&outputProto).code());
    ASSERT_TRUE(ProtoUtil::compareProto(inputProto, outputProto, [&](auto &md) {
        md.set_repeated_field_comparison(google::protobuf::util::MessageDifferencer::AS_SET);
    }));
}

TEST_F(RootTest, testCreateCatalog) {
    Root entity;
    ASSERT_EQ(Status::OK, entity.createCatalog("test").code());
    ASSERT_EQ(Status::DUPLICATE_ENTITY, entity.createCatalog("test").code());
}

TEST_F(RootTest, testDropCatalog) {
    Root entity;
    ASSERT_EQ(Status::NOT_FOUND, entity.dropCatalog("test").code());
    ASSERT_EQ(Status::OK, entity.createCatalog("test").code());
    ASSERT_EQ(Status::OK, entity.dropCatalog("test").code());
}

} // namespace catalog
