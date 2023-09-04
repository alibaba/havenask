#include "build_service/admin/catalog/CatalogVersionPublisher.h"

#include "build_service/proto/Admin.pb.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/test/unittest.h"
#include "catalog/proto/CatalogService.pb.h"
#include "google/protobuf/util/message_differencer.h"

using namespace std;
using namespace testing;

namespace catalog::proto {
bool operator==(const ::catalog::proto::UpdateBuildRequest& lhs, const ::catalog::proto::UpdateBuildRequest& rhs)
{
    google::protobuf::util::MessageDifferencer md;
    return md.Compare(lhs, rhs);
}
} // namespace catalog::proto

namespace build_service::admin {

class MockCatalogVersionPublisher : public CatalogVersionPublisher
{
public:
    MockCatalogVersionPublisher(const std::shared_ptr<CatalogPartitionIdentifier>& identifier, uint32_t generationId,
                                const std::shared_ptr<common::RpcChannelManager>& rpcChannalManager)
        : CatalogVersionPublisher(identifier, generationId, rpcChannalManager)
    {
    }
    MOCK_METHOD(bool, updateCatalog, (const catalog::proto::UpdateBuildRequest&), (override));
};

class CatalogVersionPublisherTest : public BUILD_SERVICE_TESTBASE
{
};

TEST_F(CatalogVersionPublisherTest, testSimple)
{
    auto identifier = make_shared<CatalogPartitionIdentifier>("part1", "tb1", "db1", "ct1");
    MockCatalogVersionPublisher versionPublisher(identifier, 1, nullptr);
    proto::GenerationInfo generationInfo;
    versionPublisher.publish(generationInfo);
    ASSERT_TRUE(versionPublisher._publishedShards.empty());
    ::catalog::proto::UpdateBuildRequest request;
    auto buildId = request.mutable_build()->mutable_build_id();
    buildId->set_generation_id(1);
    buildId->set_partition_name("part1");
    buildId->set_table_name("tb1");
    buildId->set_database_name("db1");
    buildId->set_catalog_name("ct1");
    request.mutable_build()->mutable_current()->set_index_root("/index");
    auto shard = request.mutable_build()->mutable_current()->add_shards();
    shard->mutable_shard_range()->set_from(0);
    shard->mutable_shard_range()->set_to(65535);
    shard->set_index_version(1);
    shard->set_index_version_timestamp(2);

    // full
    EXPECT_CALL(versionPublisher, updateCatalog(request)).WillOnce(Return(true));
    generationInfo.set_indexroot("/index");
    auto fullindexinfos = generationInfo.add_fullindexinfos();
    fullindexinfos->mutable_range()->set_from(0);
    fullindexinfos->mutable_range()->set_to(65535);
    fullindexinfos->set_indexversion(1);
    fullindexinfos->set_versiontimestamp(2);
    versionPublisher.publish(generationInfo);
    ASSERT_EQ(1, versionPublisher._publishedShards.size());
    versionPublisher.publish(generationInfo);
    // inc
    shard->set_index_version(3);
    shard->set_index_version_timestamp(4);
    EXPECT_CALL(versionPublisher, updateCatalog(request)).WillOnce(Return(true));
    auto indexinfos = generationInfo.add_indexinfos();
    indexinfos->mutable_range()->set_from(0);
    indexinfos->mutable_range()->set_to(65535);
    indexinfos->set_indexversion(3);
    indexinfos->set_versiontimestamp(4);
    versionPublisher.publish(generationInfo);
    ASSERT_EQ(1, versionPublisher._publishedShards.size());
    versionPublisher.publish(generationInfo);
}

TEST_F(CatalogVersionPublisherTest, invalidVersion)
{
    auto identifier = make_shared<CatalogPartitionIdentifier>("part1", "tb1", "db1", "ct1");
    MockCatalogVersionPublisher versionPublisher(identifier, 1, nullptr);
    proto::GenerationInfo generationInfo;
    auto fullindexinfos = generationInfo.add_fullindexinfos();
    fullindexinfos->mutable_range()->set_from(0);
    fullindexinfos->mutable_range()->set_to(65535);
    versionPublisher.publish(generationInfo);
    ASSERT_EQ(0, versionPublisher._publishedShards.size());
}

} // namespace build_service::admin
