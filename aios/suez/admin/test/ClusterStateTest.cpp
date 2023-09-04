#include "fslib/fs/zookeeper/test/ZkServer.h"
#include "google/protobuf/util/message_differencer.h"
#include "suez/admin/ClusterServiceState.h"
#include "unittest/unittest.h"

namespace suez {

class ClusterStateTest : public TESTBASE {
    void setUp() override {
        unsigned short port = fslib::fs::zookeeper::ZkServer::getZkServer()->port();
        std::ostringstream oss;
        oss << "127.0.0.1:" << port;
        _zkHost = oss.str();
    }

    void tearDown() override { fslib::fs::zookeeper::ZkServer::getZkServer()->stop(); }

protected:
    std::string _zkHost;
};

TEST_F(ClusterStateTest, testSimple) {
    // std::map<std::string, Cluster> clusterMap;
    // Cluster cluster1;
    // cluster1.set_clustername("qrs");
    // cluster1.set_version(100);
    // cluster1.set_hippoconfig("hippo_config");
    // Cluster cluster2;
    // cluster2.set_clustername("searcher");
    // cluster2.set_version(100);
    // cluster2.set_hippoconfig("hippo_config");
    // clusterMap.emplace("qrs", cluster1);
    // clusterMap.emplace("searcher", cluster2);

    // {
    //     auto state = ClusterState::create("LOCAL://" + GET_TEMPLATE_DATA_PATH());
    //     ASSERT_TRUE(state->persist(clusterMap));
    //     auto s = state->recover();
    //     ASSERT_TRUE(s.is_ok());
    //     auto result = s.get();

    //     ASSERT_EQ(2, result.size());
    //     ASSERT_EQ(1, result.count("qrs"));
    //     ASSERT_EQ(1, result.count("searcher"));
    //     ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(cluster1, result["qrs"]));
    //     ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(cluster2, result["searcher"]));
    // }

    // {
    //     auto state = ClusterState::create("zfs://" + _zkHost + "/" + GET_CLASS_NAME());
    //     ASSERT_TRUE(state->persist(clusterMap));
    //     auto s = state->recover();
    //     ASSERT_TRUE(s.is_ok());
    //     auto result = s.get();

    //     ASSERT_EQ(2, result.size());
    //     ASSERT_EQ(1, result.count("qrs"));
    //     ASSERT_EQ(1, result.count("searcher"));
    //     ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(cluster1, result["qrs"]));
    //     ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(cluster2, result["searcher"]));
    // }
}

} // namespace suez
