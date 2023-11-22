
#include "build_service/common/IndexTaskRequestQueue.h"

#include "unittest/unittest.h"

using namespace indexlibv2::framework;

namespace build_service { namespace common {

class IndexTaskRequestQueueTest : public TESTBASE
{
};

TEST_F(IndexTaskRequestQueueTest, testSimple)
{
    std::map<std::string, std::string> params;
    std::string requestType = "test_type";
    IndexTaskMetaCreator creator;
    auto meta = creator.TaskType(requestType).TaskTraceId("name").Create();

    IndexTaskRequestQueue requestQueue;
    std::string clusterName = "test_cluster";
    proto::Range range;
    range.set_from(0);
    range.set_to(32767);
    ASSERT_TRUE(requestQueue.add(clusterName, range, meta).IsOK());
    std::string key = IndexTaskRequestQueue::genPartitionLevelKey(clusterName, range);
    ASSERT_EQ(requestQueue[key].size(), 1u);
    // skip duplicate request
    ASSERT_TRUE(requestQueue.add(clusterName, range, meta).IsOK());
    ASSERT_EQ(requestQueue[key].size(), 1u);
    ASSERT_EQ(requestQueue["foo"].size(), 0);
}

TEST_F(IndexTaskRequestQueueTest, testOrder)
{
    IndexTaskMetaCreator creator;

    std::map<std::string, std::string> params1;
    params1["test1"] = "test1";
    std::string requestType = "test_type";
    auto meta1 = creator.TaskType(requestType).TaskTraceId("name1").Params(params1).EventTimeInSecs(10).Create();

    std::map<std::string, std::string> params2;
    params2["test2"] = "test2";
    auto meta2 = creator.TaskType(requestType).TaskTraceId("name2").Params(params2).EventTimeInSecs(5).Create();

    std::map<std::string, std::string> params3;
    params3["test3"] = "test3";
    auto meta3 = creator.TaskType(requestType).TaskTraceId("name3").Params(params3).EventTimeInSecs(10).Create();

    IndexTaskRequestQueue requestQueue;
    std::string clusterName = "test_cluster";
    proto::Range range;
    range.set_from(0);
    range.set_to(32767);
    ASSERT_TRUE(requestQueue.add(clusterName, range, meta1).IsOK());
    ASSERT_TRUE(requestQueue.add(clusterName, range, meta2).IsOK());
    ASSERT_TRUE(requestQueue.add(clusterName, range, meta3).IsOK());
    std::string key = IndexTaskRequestQueue::genPartitionLevelKey(clusterName, range);

    auto indexTasks = requestQueue[key];
    ASSERT_EQ(indexTasks.size(), 3u);
    ASSERT_EQ(indexTasks[0].GetEventTimeInSecs(), 5);
    ASSERT_EQ(indexTasks[0].GetParams(), params2);
    ASSERT_EQ(indexTasks[1].GetEventTimeInSecs(), 10);
    ASSERT_EQ(indexTasks[1].GetParams(), params1);
    ASSERT_EQ(indexTasks[2].GetEventTimeInSecs(), 10);
    ASSERT_EQ(indexTasks[2].GetParams(), params3);
}

TEST_F(IndexTaskRequestQueueTest, testGenPartitionKey)
{
    std::string clusterName = "test_cluster";
    proto::Range range;
    range.set_from(0);
    range.set_to(32767);
    std::string key = IndexTaskRequestQueue::genPartitionLevelKey(clusterName, range);
    ASSERT_EQ(key, "test_cluster_0_32767");
}

}} // namespace build_service::common
