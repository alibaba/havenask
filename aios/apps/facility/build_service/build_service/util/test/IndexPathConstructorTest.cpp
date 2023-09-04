#include "build_service/util/IndexPathConstructor.h"

#include "autil/StringUtil.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/proto/test/ProtoCreator.h"
#include "build_service/test/unittest.h"

using namespace std;
using namespace autil;
using namespace testing;
using namespace build_service::proto;

namespace build_service { namespace util {

class IndexPathConstructorTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void IndexPathConstructorTest::setUp() {}

void IndexPathConstructorTest::tearDown() {}

TEST_F(IndexPathConstructorTest, testConstructIndexPath)
{
    string indexRoot = GET_TEST_DATA_PATH() + "index_path_constructor_test";
    string clusterName = "simple";
    string emptyClusterName = "empty_cluster";
    string rangeFrom = "0";
    string rangeTo = "1";
    {
        // full mode, no generation
        string buildMode = "full";
        string result =
            IndexPathConstructor::getGenerationId(indexRoot, emptyClusterName, buildMode, rangeFrom, rangeTo);
        EXPECT_EQ("0", result);
    }
    {
        // full mode, generation 1
        string buildMode = "full";
        string result = IndexPathConstructor::getGenerationId(indexRoot, clusterName, buildMode, rangeFrom, rangeTo);
        EXPECT_EQ("1", result);
    }
    {
        // not full mode, no generation
        string buildMode = "not_full";
        string result = IndexPathConstructor::getGenerationId(indexRoot, clusterName, buildMode, rangeFrom, rangeTo);
        EXPECT_EQ("", result);
    }
    {
        // not full mode, partition not consistent
        string buildMode = "not_full";
        string result = IndexPathConstructor::getGenerationId(indexRoot, emptyClusterName, buildMode, "5", "6");
        EXPECT_EQ("", result);
    }
    {
        // not full mode, partition consistent
        string buildMode = "not_full";
        string result = IndexPathConstructor::getGenerationId(indexRoot, "partition_cluster", buildMode, "0", "2");
        EXPECT_EQ("1", result);
    }
    {
        // not full mode, partition consistent
        string buildMode = "not_full";
        string result = IndexPathConstructor::getGenerationId(indexRoot, "partition_cluster", buildMode, "2", "3");
        EXPECT_EQ("0", result);
    }
    {
        // not full mode, partition consistent
        string buildMode = "not_full";
        string result = IndexPathConstructor::getGenerationId(indexRoot, "partition_cluster", buildMode, "3", "4");
        EXPECT_EQ("1", result);
    }
}

TEST_F(IndexPathConstructorTest, testGetGenerationList)
{
    string indexRoot = GET_TEST_DATA_PATH() + "index_path_constructor_test";
    {
        string clusterName = "gen_neg_cluster";
        vector<int32_t> gens = IndexPathConstructor::getGenerationList(indexRoot, clusterName);
        EXPECT_THAT(gens, ElementsAre());
    }
    {
        string clusterName = "gen_gt_intmax_cluster";
        vector<int32_t> gens = IndexPathConstructor::getGenerationList(indexRoot, clusterName);
        EXPECT_THAT(gens, ElementsAre());
    }
    {
        string clusterName = "gen_is_file_cluster";
        vector<int32_t> gens = IndexPathConstructor::getGenerationList(indexRoot, clusterName);
        EXPECT_THAT(gens, ElementsAre(0));
    }
}

TEST_F(IndexPathConstructorTest, testGetNextGenerationId)
{
    string indexRoot = GET_TEST_DATA_PATH() + "index_path_constructor_test";
    {
        string clusterName = "empty_cluster";
        EXPECT_EQ(generationid_t(0), IndexPathConstructor::getNextGenerationId(indexRoot, clusterName));
    }
    {
        string clusterName = "partition_cluster";
        EXPECT_EQ(generationid_t(2), IndexPathConstructor::getNextGenerationId(indexRoot, clusterName));
    }
}

TEST_F(IndexPathConstructorTest, testGetNextGenerationIdClusters)
{
    string indexRoot = GET_TEST_DATA_PATH() + "index_path_constructor_test";
    vector<string> clusters;
    clusters.push_back("empty_cluster");
    clusters.push_back("partition_cluster");
    EXPECT_EQ(generationid_t(2), IndexPathConstructor::getNextGenerationId(indexRoot, clusters));
}

TEST_F(IndexPathConstructorTest, testGetMergerCheckpointPath)
{
    string indexRoot = GET_TEST_DATA_PATH() + "index_path_constructor_test";
    string clusterName = "simple";
    string emptyClusterName = "empty_cluster";
    string rangeFrom = "0";
    string rangeTo = "1";
    auto pid = ProtoCreator::createPartitionId(ROLE_MERGER, BUILD_STEP_FULL, 0, 2047, "simple", 1, clusterName);
    string checkpointPath = IndexPathConstructor::getMergerCheckpointPath(indexRoot, pid);
    string roleId;
    ProtoUtil::partitionIdToRoleId(pid, roleId);
    vector<string> paths = {indexRoot, clusterName, "generation_1", roleId + ".checkpoint"};
    string expected = StringUtil::toString(paths, "/");
    ASSERT_EQ(expected, checkpointPath);
}

}} // namespace build_service::util
