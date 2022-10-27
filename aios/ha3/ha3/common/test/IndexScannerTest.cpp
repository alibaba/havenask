#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/IndexScanner.h>
#include <ha3/test/test.h>
#include <suez/turing/common/FileUtil.h>
#include <fslib/fslib.h>

using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace suez::turing;

USE_HA3_NAMESPACE(proto);

BEGIN_HA3_NAMESPACE(common);

class IndexScannerTest : public TESTBASE {
public:
    IndexScannerTest();
    ~IndexScannerTest();
public:
    void setUp();
    void tearDown();
protected:
    proto::Range createRange(uint16_t from, uint16_t to);
protected:
    std::string _rootPath;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, IndexScannerTest);


IndexScannerTest::IndexScannerTest() {
    _rootPath = GET_TEMP_DATA_PATH() + "IndexScannerTest";
}

IndexScannerTest::~IndexScannerTest() {
}

void IndexScannerTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    FileSystem::remove(_rootPath);
}

void IndexScannerTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
    FileSystem::remove(_rootPath);
}

TEST_F(IndexScannerTest, testOfflineScanIndex) {
    HA3_LOG(DEBUG, "Begin Test!");

    FileUtil::writeToFile(_rootPath + "/daogou/generation_0/partition_1_2/version.2", "");
    FileUtil::writeToFile(_rootPath + "/daogou/generation_0/partition_2_3/version.1", "");
    FileUtil::writeToFile(_rootPath + "/daogou/generation_0/partition_2_3/version.3", "");
    FileUtil::writeToFile(_rootPath + "/daogou/generation_1/partition_0_3/version.10", "");
    FileUtil::writeToFile(_rootPath + "/daogou/generation_1/partition_4_10/version.7", "");
    FileUtil::writeToFile(_rootPath + "/clustername/generation_1/partition_0_1/version.1", "");
    FileUtil::writeToFile(_rootPath + "/not_dir", "");
    FileUtil::writeToFile(_rootPath + "/daogou/generation_2", "");
    FileUtil::writeToFile(_rootPath + "/daogou/generation_3/partition_1_2", "");
    FileUtil::writeToFile(_rootPath + "/daogou/generation_3/partition_1_3/version.11.1", "");
    FileUtil::writeToFile(_rootPath + "/daogou/generation_3/partition_1_3/segment_1/version.1", "");

    Range range;

    IndexScanner::IndexInfo indexInfo;
    ASSERT_TRUE(IndexScanner::scanIndex(_rootPath, indexInfo, true));
    ASSERT_EQ((size_t)2, indexInfo.size());
    ASSERT_TRUE(indexInfo.find("daogou") != indexInfo.end());
    IndexScanner::ClusterIndexInfo &clusterInfo = indexInfo["daogou"];
    ASSERT_EQ((size_t)3, clusterInfo.size());

    ASSERT_TRUE(clusterInfo.find(0) != clusterInfo.end());
    ASSERT_EQ((size_t)0, clusterInfo[0].size());

    ASSERT_TRUE(clusterInfo.find(1) != clusterInfo.end());
    ASSERT_EQ((size_t)2, clusterInfo[1].size());
    range = createRange(0, 3);
    ASSERT_EQ((int32_t)10, clusterInfo[1][range]);
    range = createRange(4, 10);
    ASSERT_EQ((int32_t)7, clusterInfo[1][range]);

    ASSERT_TRUE(clusterInfo.find(3) != clusterInfo.end());
    ASSERT_EQ((size_t)0, clusterInfo[3].size());

    ASSERT_TRUE(indexInfo.find("clustername") != indexInfo.end());
    clusterInfo = indexInfo["clustername"];
    ASSERT_TRUE(clusterInfo.find(1) != clusterInfo.end());
    ASSERT_EQ((size_t)1, clusterInfo[1].size());
    range = createRange(0, 1);
    ASSERT_EQ((int32_t)1, clusterInfo[1][range]);
}

TEST_F(IndexScannerTest, testOfflineScanIndexWith2Level) {
    HA3_LOG(DEBUG, "Begin Test!");
    FileUtil::writeToFile(_rootPath + "/daogou/generation_0/partition_1_3/version.1", "");
    FileUtil::writeToFile(_rootPath + "/daogou/generation_1/partition_1_3/version.1", "");
    FileUtil::writeToFile(_rootPath + "/daogou/generation_2/partition_1_3/version.1", "");
    FileUtil::writeToFile(_rootPath + "/daogou/generation_3/partition_1_3/version.1", "");

    Range range;

    IndexScanner::ClusterIndexInfo clusterInfo;
    ASSERT_TRUE(IndexScanner::scanCluster(_rootPath + "/daogou", clusterInfo, true, 2));
    ASSERT_EQ(size_t(4), clusterInfo.size());

    ASSERT_EQ(size_t(0), clusterInfo[0].size());
    ASSERT_EQ(size_t(0), clusterInfo[1].size());
    ASSERT_EQ(size_t(1), clusterInfo[2].size());
    ASSERT_EQ(size_t(1), clusterInfo[3].size());
}

TEST_F(IndexScannerTest, testScanIndexWithFile) {
    HA3_LOG(DEBUG, "Begin Test!");
    FileUtil::writeToFile(_rootPath, "");
    IndexScanner::IndexInfo indexInfo;
    ASSERT_TRUE(!IndexScanner::scanIndex(_rootPath, indexInfo));
}

TEST_F(IndexScannerTest, testScanIndexWithEmptyDir) {
    HA3_LOG(DEBUG, "Begin Test!");
    FileSystem::mkDir(_rootPath);
    IndexScanner::IndexInfo indexInfo;
    ASSERT_TRUE(IndexScanner::scanIndex(_rootPath, indexInfo));
    ASSERT_EQ((size_t)0, indexInfo.size());
}

TEST_F(IndexScannerTest, testOfflineCluster) {
    HA3_LOG(DEBUG, "Begin Test!");
    FileUtil::writeToFile(_rootPath + "/_ha3_hd_builder_tmp_dir_/xxxx", "");
    FileUtil::writeToFile(_rootPath + "/_hadoop_build_job.info", "");
    FileUtil::writeToFile(_rootPath + "/generation_0/partition_1_2/version.2", "");
    FileUtil::writeToFile(_rootPath + "/generation_0/partition_2_3/version.1", "");
    FileUtil::writeToFile(_rootPath + "/generation_0/partition_2_3/version.3", "");
    FileUtil::writeToFile(_rootPath + "/generation_1/partition_0_3/version.10", "");
    FileUtil::writeToFile(_rootPath + "/generation_1/partition_4_10/version.7", "");
    FileUtil::writeToFile(_rootPath + "/generation_2", "");
    FileUtil::writeToFile(_rootPath + "/generation_3/partition_1_2", "");
    FileUtil::writeToFile(_rootPath + "/generation_3/partition_1_3/version.11.1", "");
    FileUtil::writeToFile(_rootPath + "/generation_3/partition_1_3/segment_1/version.1", "");

    Range range;

    IndexScanner::ClusterIndexInfo clusterInfo;
    ASSERT_TRUE(IndexScanner::scanCluster(_rootPath, clusterInfo, true));

    ASSERT_EQ((size_t)3, clusterInfo.size());

    ASSERT_TRUE(clusterInfo.find(0) != clusterInfo.end());
    ASSERT_EQ((size_t)0, clusterInfo[0].size());

    ASSERT_TRUE(clusterInfo.find(1) != clusterInfo.end());
    ASSERT_EQ((size_t)2, clusterInfo[1].size());
    range = createRange(0, 3);
    ASSERT_EQ((int32_t)10, clusterInfo[1][range]);
    range = createRange(4, 10);
    ASSERT_EQ((int32_t)7, clusterInfo[1][range]);

    ASSERT_TRUE(clusterInfo.find(3) != clusterInfo.end());
    ASSERT_EQ((size_t)0, clusterInfo[3].size());
}

TEST_F(IndexScannerTest, testOfflineScannerCluster) {
    HA3_LOG(DEBUG, "Begin Test!");
    FileUtil::writeToFile(_rootPath + "/generation_2/partition_0_3000/version.10", "");
    FileUtil::writeToFile(_rootPath + "/generation_2/partition_5000_65535/version.10", "");
    FileUtil::writeToFile(_rootPath + "/generation_1/partition_0_1999/version.9", "");
    FileUtil::writeToFile(_rootPath + "/generation_1/partition_2000_6000/version.9", "");
    FileUtil::writeToFile(_rootPath + "/generation_1/partition_6001_65535/version.9", "");
    FileUtil::writeToFile(_rootPath + "/generation_0/partition_0_5000/version.8", "");
    FileUtil::writeToFile(_rootPath + "/generation_0/partition_5001_65535/version.8", "");

    Range range;
    IndexScanner::ClusterIndexInfo clusterInfo;
    ASSERT_TRUE(IndexScanner::scanCluster(_rootPath, clusterInfo, true));
    ASSERT_EQ((size_t)2, clusterInfo.size());
    ASSERT_TRUE(clusterInfo.find(2) != clusterInfo.end());
    ASSERT_EQ((size_t)2, clusterInfo[2].size());
    range = createRange(0, 3000);
    ASSERT_EQ((int32_t)10, clusterInfo[2][range]);
    range = createRange(5000, 65535);
    ASSERT_EQ((int32_t)10, clusterInfo[2][range]);
    ASSERT_TRUE(clusterInfo.find(1) != clusterInfo.end());
    ASSERT_EQ((size_t)1, clusterInfo[1].size());
    range = createRange(2000, 6000);
    ASSERT_EQ((int32_t)9, clusterInfo[1][range]);
}

TEST_F(IndexScannerTest, testOfflineScannerClusterWithCompleteGeneration) {
    HA3_LOG(DEBUG, "Begin Test!");
    FileUtil::writeToFile(_rootPath + "/generation_3/partition_0_65535/version.9", "");
    FileUtil::writeToFile(_rootPath + "/generation_2/partition_0_65535/version.9", "");
    FileUtil::writeToFile(_rootPath + "/generation_1/partition_0_65535/version.9", "");
    FileUtil::writeToFile(_rootPath + "/generation_0/partition_0_65535/version.9", "");

    Range range;
    IndexScanner::ClusterIndexInfo clusterInfo;
    ASSERT_TRUE(IndexScanner::scanCluster(_rootPath, clusterInfo, true));
    ASSERT_EQ((size_t)1, clusterInfo.size());
    ASSERT_TRUE(clusterInfo.find(3) != clusterInfo.end());
    ASSERT_EQ((size_t)1, clusterInfo[3].size());
    range = createRange(0, 65535);
    ASSERT_EQ((int32_t)9, clusterInfo[3][range]);
}

TEST_F(IndexScannerTest, testClusterWithFile) {
    HA3_LOG(DEBUG, "Begin Test!");
    FileUtil::writeToFile(_rootPath, "");
    IndexScanner::ClusterIndexInfo cluster;
    ASSERT_TRUE(!IndexScanner::scanCluster(_rootPath, cluster));
}

TEST_F(IndexScannerTest, testClusterWithEmptyDir) {
    HA3_LOG(DEBUG, "Begin Test!");
    FileSystem::mkDir(_rootPath);
    IndexScanner::ClusterIndexInfo cluster;
    ASSERT_TRUE(IndexScanner::scanCluster(_rootPath, cluster));
    ASSERT_EQ((size_t)0, cluster.size());
}

TEST_F(IndexScannerTest, testScanClusterNames) {
    HA3_LOG(DEBUG, "Begin Test!");

    vector<string> clusterNames;
    ASSERT_TRUE(!IndexScanner::scanClusterNames(_rootPath, clusterNames));

    FileSystem::mkDir(_rootPath);
    ASSERT_TRUE(IndexScanner::scanClusterNames(_rootPath, clusterNames));
    ASSERT_EQ((size_t)0, clusterNames.size());

    string cluster1 = _rootPath + "/cluster1";
    string cluster2 = _rootPath + "/cluster2";
    string cluster3 = _rootPath + "/cluster3";
    string file1 = _rootPath + "/file1";
    FileSystem::mkDir(cluster1);
    FileSystem::mkDir(cluster2);
    FileSystem::mkDir(cluster3);
    FileUtil::writeToFile(file1, "");

    ASSERT_TRUE(IndexScanner::scanClusterNames(_rootPath, clusterNames));
    ASSERT_EQ((size_t)3, clusterNames.size());
    sort(clusterNames.begin(), clusterNames.end());
    ASSERT_EQ(string("cluster1"), clusterNames[0]);
    ASSERT_EQ(string("cluster2"), clusterNames[1]);
    ASSERT_EQ(string("cluster3"), clusterNames[2]);
}

TEST_F(IndexScannerTest, testOfflineGeneration) {
    HA3_LOG(DEBUG, "Begin Test!");
    FileUtil::writeToFile(_rootPath + "/_ha3_hd_builder_tmp_dir_/xxxx", "");
    FileUtil::writeToFile(_rootPath + "/_hadoop_build_job.info", "");
    FileUtil::writeToFile(_rootPath + "/partition_0_1/version.1", "");
    FileUtil::writeToFile(_rootPath + "/partition_0_1/version.3", "");
    FileUtil::writeToFile(_rootPath + "/partition_0_1/version.3.4", "");
    FileUtil::writeToFile(_rootPath + "/partition_1_2", "");
    FileUtil::writeToFile(_rootPath + "/partition_1_3/segment_1/version.1", "");

    Range range;

    IndexScanner::GenerationInfo info;
    MultiLevelRangeSet rangeSet1(1);
    ASSERT_TRUE(IndexScanner::scanGeneration(_rootPath, info, rangeSet1, true));
    ASSERT_TRUE(!rangeSet1.isComplete());
    ASSERT_EQ((size_t)1, info.size());
    range = createRange(0, 1);
    ASSERT_EQ((int32_t)3, info[range]);


    FileUtil::writeToFile(_rootPath + "/partition_2_3000/version.3", "");
    FileUtil::writeToFile(_rootPath + "/partition_3001_65535/version.4", "");
    info.clear();
    MultiLevelRangeSet rangeSet2(1);
    ASSERT_TRUE(rangeSet2.push(2, 4000));
    ASSERT_TRUE(!rangeSet2.isComplete());
    ASSERT_TRUE(IndexScanner::scanGeneration(_rootPath, info, rangeSet2, true));
    ASSERT_TRUE(rangeSet2.isComplete());
    ASSERT_EQ((size_t)2, info.size());
    range = createRange(0, 1);
    ASSERT_EQ((int32_t)3, info[range]);
    range = createRange(3001, 65535);
    ASSERT_EQ((int32_t)4, info[range]);

    info.clear();
    MultiLevelRangeSet rangeSet3(1);
    ASSERT_TRUE(IndexScanner::scanGeneration(_rootPath, info, rangeSet3, true));
    ASSERT_TRUE(rangeSet3.isComplete());
    ASSERT_EQ((size_t)3, info.size());
    range = createRange(0, 1);
    ASSERT_EQ((int32_t)3, info[range]);
    range = createRange(2, 3000);
    ASSERT_EQ((int32_t)3, info[range]);
    range = createRange(3001, 65535);
    ASSERT_EQ((int32_t)4, info[range]);

    info.clear();
    MultiLevelRangeSet rangeSet4(1);
    ASSERT_TRUE(rangeSet4.push(0, 65535));
    ASSERT_TRUE(rangeSet4.isComplete());
    ASSERT_TRUE(IndexScanner::scanGeneration(_rootPath, info, rangeSet4, true));
    ASSERT_TRUE(rangeSet4.isComplete());
    ASSERT_EQ((size_t)0, info.size());
}

TEST_F(IndexScannerTest, testGenerationWithFile) {
    HA3_LOG(DEBUG, "Begin Test!");
    FileUtil::writeToFile(_rootPath, "");
    IndexScanner::GenerationInfo info;
    MultiLevelRangeSet rangeSet(1);
    ASSERT_TRUE(!IndexScanner::scanGeneration(_rootPath, info, rangeSet));
    ASSERT_TRUE(!rangeSet.isComplete());
}

TEST_F(IndexScannerTest, testGenerationWithEmptyDir) {
    HA3_LOG(DEBUG, "Begin Test!");
    FileSystem::mkDir(_rootPath);
    IndexScanner::GenerationInfo info;
    MultiLevelRangeSet rangeSet(1);
    ASSERT_TRUE(IndexScanner::scanGeneration(_rootPath, info, rangeSet));
    ASSERT_EQ((size_t)0, info.size());
    ASSERT_TRUE(!rangeSet.isComplete());
}

TEST_F(IndexScannerTest, testGetMaxIncrementalVersion) {
    uint32_t version = 0;
    bool hasError;
    string partitionDir = string(TEST_DATA_PATH) + "/testGetMaxIncrementalVersion";

    FileSystem::remove(partitionDir);
    ASSERT_TRUE(!IndexScanner::getMaxIncrementalVersion(partitionDir, version, hasError));
    ASSERT_TRUE(!hasError); // partition dir not exist

    FileSystem::mkDir(partitionDir);
    string wrongDir = partitionDir + "/version.2";
    FileSystem::mkDir(wrongDir);
    ASSERT_TRUE(!IndexScanner::getMaxIncrementalVersion(partitionDir, version, hasError));
    ASSERT_TRUE(hasError);

    string versionFile1 = partitionDir + "/version.0";
    string versionFile2 = partitionDir + "/version.1";
    string versionFile3 = partitionDir + "/version.3";
    FileUtil::writeToFile(versionFile1, "");
    FileUtil::writeToFile(versionFile2, "");
    FileUtil::writeToFile(versionFile3, "");
    ASSERT_TRUE(IndexScanner::getMaxIncrementalVersion(partitionDir, version, hasError));
    ASSERT_EQ((uint32_t)3, version );
    ASSERT_TRUE(!hasError);

    string wrongDir2 = partitionDir + "/version.4";
    FileSystem::mkDir(wrongDir2);
    ASSERT_TRUE(!IndexScanner::getMaxIncrementalVersion(partitionDir, version, hasError));
    ASSERT_TRUE(hasError);
    FileSystem::remove(partitionDir);
}

TEST_F(IndexScannerTest, testGetMaxIncrementalVersionWithFile)
{
    FileUtil::writeToFile(_rootPath, "");
    uint32_t version = 0;
    bool hasError;
    ASSERT_TRUE(!IndexScanner::getMaxIncrementalVersion(_rootPath, version, hasError));
}

TEST_F(IndexScannerTest, testGetMaxIncrementalVersionWithEmptyDir)
{
    uint32_t version = 0;
    bool hasError;

    ASSERT_TRUE(!IndexScanner::getMaxIncrementalVersion("", version, hasError));
    ASSERT_TRUE(hasError);

    FileSystem::mkDir(_rootPath);
    ASSERT_TRUE(!IndexScanner::getMaxIncrementalVersion(_rootPath, version, hasError));
    ASSERT_TRUE(!hasError);
}

Range IndexScannerTest::createRange(uint16_t from, uint16_t to) {
    Range range;
    range.set_from(from);
    range.set_to(to);
    return range;
}

TEST_F(IndexScannerTest, testOnlineScanCluster) {
    HA3_LOG(DEBUG, "Begin Test!");
    FileUtil::writeToFile(_rootPath + "/generation_2/partition_0_3000/version.10", "");
    FileUtil::writeToFile(_rootPath + "/generation_2/partition_5000_65535/version.10", "");
    FileUtil::writeToFile(_rootPath + "/generation_1/partition_0_1999/version.9", "");
    FileUtil::writeToFile(_rootPath + "/generation_1/partition_2000_6000/version.9", "");
    FileUtil::writeToFile(_rootPath + "/generation_1/partition_6001_65535/version.9", "");
    FileUtil::writeToFile(_rootPath + "/generation_0/partition_0_5000/version.8", "");
    FileUtil::writeToFile(_rootPath + "/generation_0/partition_5001_65535/version.8", "");

    Range range;
    IndexScanner::ClusterIndexInfo clusterInfo;
    ASSERT_TRUE(IndexScanner::scanCluster(_rootPath, clusterInfo, false));
    ASSERT_EQ((size_t)3, clusterInfo.size());
    ASSERT_TRUE(clusterInfo.find(2) != clusterInfo.end());
    ASSERT_EQ((size_t)2, clusterInfo[2].size());
    range = createRange(0, 3000);
    ASSERT_EQ((int32_t)10, clusterInfo[2][range]);
    range = createRange(5000, 65535);
    ASSERT_EQ((int32_t)10, clusterInfo[2][range]);

    ASSERT_TRUE(clusterInfo.find(1) != clusterInfo.end());
    ASSERT_EQ((size_t)3, clusterInfo[1].size());
    range = createRange(0, 1999);
    ASSERT_EQ((int32_t)9, clusterInfo[1][range]);
    range = createRange(2000, 6000);
    ASSERT_EQ((int32_t)9, clusterInfo[1][range]);
    range = createRange(6001, 65535);
    ASSERT_EQ((int32_t)9, clusterInfo[1][range]);

    ASSERT_TRUE(clusterInfo.find(0) != clusterInfo.end());
    ASSERT_EQ((size_t)2, clusterInfo[0].size());
    range = createRange(0, 5000);
    ASSERT_EQ((int32_t)8, clusterInfo[0][range]);
    range = createRange(5001, 65535);
    ASSERT_EQ((int32_t)8, clusterInfo[0][range]);
}


TEST_F(IndexScannerTest, testIsGenerationValid) {
    HA3_LOG(DEBUG, "Begin Test!");
    string generationPath = _rootPath + "/daogou/generation_0/";
    FileUtil::writeToFile(generationPath + "version.2", "");
    IndexScanner indexScanner;
    ASSERT_TRUE(indexScanner.isGenerationValid(generationPath));
    FileUtil::writeToFile(generationPath + IndexScanner::INDEX_INVALID_FLAG, "");
    ASSERT_TRUE(!indexScanner.isGenerationValid(generationPath));
}

TEST_F(IndexScannerTest, testOnlineScanClusterWithGenerationInvalid) {
    HA3_LOG(DEBUG, "Begin Test!");
    FileUtil::writeToFile(_rootPath + "/generation_2/partition_0_3000/version.10", "");
    FileUtil::writeToFile(_rootPath + "/generation_2/partition_3001_65535/version.10", "");
    FileUtil::writeToFile(_rootPath + "/generation_2/" + IndexScanner::INDEX_INVALID_FLAG, "");
    FileUtil::writeToFile(_rootPath + "/generation_1/partition_0_1999/version.9", "");
    FileUtil::writeToFile(_rootPath + "/generation_1/partition_2000_6000/version.9", "");
    FileUtil::writeToFile(_rootPath + "/generation_1/partition_6001_65535/version.9", "");
    FileUtil::writeToFile(_rootPath + "/generation_0/partition_0_5000/version.8", "");
    FileUtil::writeToFile(_rootPath + "/generation_0/partition_5001_65535/version.8", "");
    FileUtil::writeToFile(_rootPath + "/generation_0/" + IndexScanner::INDEX_INVALID_FLAG, "");

    Range range;
    IndexScanner::ClusterIndexInfo clusterInfo;
    ASSERT_TRUE(IndexScanner::scanCluster(_rootPath, clusterInfo, false));
    ASSERT_EQ((size_t)1, clusterInfo.size());
    ASSERT_TRUE(clusterInfo.find(1) != clusterInfo.end());
    ASSERT_EQ((size_t)3, clusterInfo[1].size());
}

TEST_F(IndexScannerTest, testScanIndexWithInvalidCluster) {
    HA3_LOG(DEBUG, "Begin Test!");

    FileUtil::writeToFile(_rootPath + "/c1/generation_0/partition_1_2/version.2", "");
    FileUtil::writeToFile(_rootPath + "/c2/generation_0/partition_1_2/version.2/xx", "");
    FileUtil::writeToFile(_rootPath + "/c3/generation_0/partition_1_2/version.2", "");

    IndexScanner::IndexInfo indexInfo;
    ASSERT_TRUE(!IndexScanner::scanIndex(_rootPath, indexInfo));
    ASSERT_EQ((size_t)2, indexInfo.size());
    ASSERT_TRUE(indexInfo.find("c1") != indexInfo.end());
    IndexScanner::ClusterIndexInfo &clusterInfo = indexInfo["c1"];
    ASSERT_EQ((size_t)1, clusterInfo.size());

    ASSERT_TRUE(indexInfo.find("c3") != indexInfo.end());
    clusterInfo = indexInfo["c3"];
    ASSERT_EQ((size_t)1, clusterInfo.size());
}

TEST_F(IndexScannerTest, testScanClusterWithInvalidGeneration) {
    HA3_LOG(DEBUG, "Begin Test!");
    FileUtil::writeToFile(_rootPath + "/daogou/generation_0/partition_1_3/version.1", "");
    FileUtil::writeToFile(_rootPath + "/daogou/generation_1/partition_1_3/version.1", "");
    FileUtil::writeToFile(_rootPath + "/daogou/generation_2/partition_1_3/version.1/xx", "");
    FileUtil::writeToFile(_rootPath + "/daogou/generation_3/partition_1_3/version.1", "");

    Range range;

    IndexScanner::ClusterIndexInfo clusterInfo;
    ASSERT_TRUE(!IndexScanner::scanCluster(_rootPath + "/daogou", clusterInfo));

    ASSERT_EQ(size_t(3), clusterInfo.size());
    ASSERT_TRUE(clusterInfo.find(0) != clusterInfo.end());
    ASSERT_TRUE(clusterInfo.find(1) != clusterInfo.end());
    ASSERT_TRUE(clusterInfo.find(3) != clusterInfo.end());
    ASSERT_EQ(size_t(1), clusterInfo[0].size());
    ASSERT_EQ(size_t(1), clusterInfo[1].size());
    ASSERT_EQ(size_t(1), clusterInfo[3].size());
}

END_HA3_NAMESPACE(common);
