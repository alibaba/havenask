#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/ClusterClause.h>
#include <ha3/common/ClusterClause.h>

using namespace std;

BEGIN_HA3_NAMESPACE(common);

class ClusterClauseTest : public TESTBASE {
public:
    ClusterClauseTest();
    ~ClusterClauseTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, ClusterClauseTest);


ClusterClauseTest::ClusterClauseTest() {
}

ClusterClauseTest::~ClusterClauseTest() {
}

void ClusterClauseTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void ClusterClauseTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(ClusterClauseTest, testEmptyPartIds) {
    {
        ClusterClause clusterClause;
        ASSERT_TRUE(clusterClause.emptyPartIds());
    }
    {
        ClusterClause clusterClause;
        vector<hashid_t> partids_1;
        clusterClause.addClusterPartIds(string("cluster1"), partids_1);
        ASSERT_TRUE(clusterClause.emptyPartIds());
    }
    {
        ClusterClause clusterClause;
        vector<hashid_t> partids_1;
        partids_1.push_back((hashid_t)0);
        partids_1.push_back((hashid_t)1);
        clusterClause.addClusterPartIds(string("cluster1"), partids_1);
        ASSERT_FALSE(clusterClause.emptyPartIds());
    }
    {
        ClusterClause clusterClause;
        vector<pair<hashid_t, hashid_t> > partids_2;
        clusterClause.addClusterPartIds(string("cluster2"), partids_2);
        ASSERT_TRUE(clusterClause.emptyPartIds());
    }
    {
        ClusterClause clusterClause;
        vector<pair<hashid_t, hashid_t> > partids_2;
        partids_2.push_back({0, 0});
        partids_2.push_back({1, 1});
        partids_2.push_back({2, 3});
        clusterClause.addClusterPartIds(string("cluster2"), partids_2);
        ASSERT_FALSE(clusterClause.emptyPartIds());
    }
}

TEST_F(ClusterClauseTest, testAddAndGetClusterPartids) {
    HA3_LOG(DEBUG, "Begin Test!");
    ClusterClause clusterClause;
    vector<hashid_t> partids_1;
    partids_1.push_back((hashid_t)0);
    partids_1.push_back((hashid_t)1);
    clusterClause.addClusterPartIds(string("cluster1"), partids_1);
    partids_1.push_back((hashid_t)4);
    clusterClause.addClusterPartIds(string("cluster1"), partids_1);

    vector<pair<hashid_t, hashid_t> > partids_2;
    partids_2.push_back({0, 0});
    partids_2.push_back({1, 1});
    partids_2.push_back({2, 3});
    clusterClause.addClusterPartIds(string("cluster2"), partids_2);

    partids_2.clear();
    bool ret = clusterClause.getClusterPartIds(string("cluster1.default"), partids_2);
    ASSERT_TRUE(ret);
    ASSERT_EQ((size_t)3, partids_2.size());
    ASSERT_EQ((hashid_t)0, partids_2[0].first);
    ASSERT_EQ((hashid_t)0, partids_2[0].second);
    ASSERT_EQ((hashid_t)1, partids_2[1].first);
    ASSERT_EQ((hashid_t)1, partids_2[1].second);
    ASSERT_EQ((hashid_t)4, partids_2[2].first);
    ASSERT_EQ((hashid_t)4, partids_2[2].second);

    partids_2.clear();
    ret = clusterClause.getClusterPartIds(string("cluster2.default"), partids_2);
    ASSERT_TRUE(ret);
    ASSERT_EQ((size_t)3, partids_2.size());
    ASSERT_EQ((hashid_t)0, partids_2[0].first);
    ASSERT_EQ((hashid_t)0, partids_2[0].second);
    ASSERT_EQ((hashid_t)1, partids_2[1].first);
    ASSERT_EQ((hashid_t)1, partids_2[1].second);
    ASSERT_EQ((hashid_t)2, partids_2[2].first);
    ASSERT_EQ((hashid_t)3, partids_2[2].second);
}

TEST_F(ClusterClauseTest, testSerializeAndDeserialize) {
    HA3_LOG(DEBUG, "Begin Test!");
    ClusterClause clusterClause;

    string originalString = "cluster1:part_ids=0|1,cluster2:part_ids=0|1|2";
    clusterClause.setOriginalString(originalString);
    vector<hashid_t> partids_1;
    partids_1.push_back((hashid_t)0);
    partids_1.push_back((hashid_t)1);
    clusterClause.addClusterPartIds(string("cluster1"), partids_1);
    vector<pair<hashid_t, hashid_t> > partids_2;
    partids_2.push_back({0, 0});
    partids_2.push_back({1, 1});
    partids_2.push_back({2, 3});
    clusterClause.addClusterPartIds(string("cluster2"), partids_2);
    clusterClause.setTotalSearchPartCnt(10);

    ClusterClause resultClusterClause;

    autil::DataBuffer dataBuffer;
    clusterClause.serialize(dataBuffer);
    resultClusterClause.deserialize(dataBuffer);
    ASSERT_EQ((size_t)2, resultClusterClause.getClusterNameCount());
    vector<string> clusterNameVec = resultClusterClause.getClusterNameVector();
    ASSERT_EQ(string("cluster1.default"), clusterNameVec[0]);
    ASSERT_EQ(string("cluster2.default"), clusterNameVec[1]);

    ASSERT_EQ(originalString, resultClusterClause.getOriginalString());
    ASSERT_EQ(10, resultClusterClause.getToalSearchPartCnt());
    const map<string, vector<pair<hashid_t, hashid_t> > > &clusterPartIdsMap =
        resultClusterClause.getClusterPartIdPairsMap();

    ASSERT_EQ((size_t)2, clusterPartIdsMap.size());
    map<string, vector<pair<hashid_t, hashid_t> > >::const_iterator it;
    it = clusterPartIdsMap.find("cluster1.default");
    ASSERT_TRUE(it != clusterPartIdsMap.end());
    vector<pair<hashid_t, hashid_t> > partids = it->second;
    ASSERT_EQ((size_t)2, partids.size());
    ASSERT_EQ((hashid_t)0, partids[0].first);
    ASSERT_EQ((hashid_t)0, partids[0].second);
    ASSERT_EQ((hashid_t)1, partids[1].first);
    ASSERT_EQ((hashid_t)1, partids[1].second);

    it = clusterPartIdsMap.find("cluster2.default");
    ASSERT_TRUE(it != clusterPartIdsMap.end());
     partids = it->second;
    ASSERT_EQ((size_t)3, partids.size());
    ASSERT_EQ((hashid_t)0, partids[0].first);
    ASSERT_EQ((hashid_t)0, partids[0].second);
    ASSERT_EQ((hashid_t)1, partids[1].first);
    ASSERT_EQ((hashid_t)1, partids[1].second);
    ASSERT_EQ((hashid_t)2, partids[2].first);
    ASSERT_EQ((hashid_t)3, partids[2].second);

/*
    partids_1.clear();
    bool ret = resultClusterClause.getClusterPartIds(string("cluster1.default"), partids_1);
    ASSERT_TRUE(ret);
    ASSERT_EQ((size_t)2, partids_1.size());
    ASSERT_EQ((hashid_t)0, partids_1[0]);
    ASSERT_EQ((hashid_t)1, partids_1[1]);

    partids_1.clear();
    ret = resultClusterClause.getClusterPartIds(string("cluster2.default"), partids_1);
    ASSERT_TRUE(ret);
    ASSERT_EQ((size_t)4, partids_1.size());
    ASSERT_EQ((hashid_t)0, partids_1[0]);
    ASSERT_EQ((hashid_t)1, partids_1[1]);
    ASSERT_EQ((hashid_t)2, partids_1[2]);
    ASSERT_EQ((hashid_t)3, partids_1[3]);
*/
    partids_2.clear();
    bool ret = resultClusterClause.getClusterPartIds(string("cluster1.default"), partids_2);
    ASSERT_TRUE(ret);
    ASSERT_EQ((size_t)2, partids_2.size());
    ASSERT_EQ((hashid_t)0, partids_2[0].first);
    ASSERT_EQ((hashid_t)0, partids_2[0].second);
    ASSERT_EQ((hashid_t)1, partids_2[1].first);
    ASSERT_EQ((hashid_t)1, partids_2[1].second);

    partids_2.clear();
    ret = resultClusterClause.getClusterPartIds(string("cluster2.default"), partids_2);
    ASSERT_TRUE(ret);
    ASSERT_EQ((size_t)3, partids_2.size());
    ASSERT_EQ((hashid_t)0, partids_2[0].first);
    ASSERT_EQ((hashid_t)0, partids_2[0].second);
    ASSERT_EQ((hashid_t)1, partids_2[1].first);
    ASSERT_EQ((hashid_t)1, partids_2[1].second);
    ASSERT_EQ((hashid_t)2, partids_2[2].first);
    ASSERT_EQ((hashid_t)3, partids_2[2].second);
}

TEST_F(ClusterClauseTest, testNewSerializeAndOldDeserialize) {
    HA3_LOG(DEBUG, "Begin Test!");
    ClusterClause clusterClause;
    string originalString = "cluster1:part_ids=0|1";
    clusterClause.setOriginalString(originalString);
    vector<pair<hashid_t, hashid_t>> partids_1;
    partids_1.push_back({0, 0});
    partids_1.push_back({1, 1});
    partids_1.push_back({3, 4});
    clusterClause.addClusterPartIds(string("cluster1"), partids_1);
    autil::DataBuffer dataBuffer;
    clusterClause.serialize(dataBuffer);

    ClusterPartIdsMap clusterPartIdsMap;
    std::vector<std::string> clusterNameVec;
    string originalString2;
    dataBuffer.read(clusterPartIdsMap);
    dataBuffer.read(clusterNameVec);
    dataBuffer.read(originalString2);

    ASSERT_EQ((size_t)1, clusterNameVec.size());
    ASSERT_EQ(string("cluster1.default"), clusterNameVec[0]);
    ASSERT_EQ(originalString, originalString2);
    ASSERT_EQ((size_t)1, clusterPartIdsMap.size());
    auto it = clusterPartIdsMap.find("cluster1.default");
    ASSERT_TRUE(it != clusterPartIdsMap.end());
    vector<hashid_t> partids = it->second;
    ASSERT_EQ((size_t)2, partids.size());
    ASSERT_EQ((hashid_t)0, partids[0]);
    ASSERT_EQ((hashid_t)1, partids[1]);
}

TEST_F(ClusterClauseTest, testOldSerializeAndNewDeserialize) {
    HA3_LOG(DEBUG, "Begin Test!");
    autil::DataBuffer dataBuffer;
    string originalString = "cluster1:part_ids=0|1";
    {
        ClusterPartIdsMap clusterPartIdsMap;
        vector<hashid_t> partids = {0, 1};
        clusterPartIdsMap["cluster1.default"] = partids;
        std::vector<std::string> clusterNameVec;
        clusterNameVec.push_back("cluster1.default");
        dataBuffer.write(clusterPartIdsMap);
        dataBuffer.write(clusterNameVec);
        dataBuffer.write(originalString);
    }
    ClusterClause resultClusterClause;
    resultClusterClause.deserialize(dataBuffer);
    ASSERT_EQ((size_t)1, resultClusterClause.getClusterNameCount());
    vector<string> clusterNameVec = resultClusterClause.getClusterNameVector();
    ASSERT_EQ(string("cluster1.default"), clusterNameVec[0]);

    ASSERT_EQ(originalString, resultClusterClause.getOriginalString());
    auto clusterPartIdPairsMap = resultClusterClause.getClusterPartIdPairsMap();
    ASSERT_EQ((size_t)0, clusterPartIdPairsMap.size());

    {
        auto clusterPartIdsMap = resultClusterClause.getClusterPartIdsMap();
        ASSERT_EQ((size_t)1, clusterPartIdsMap.size());
        vector<hashid_t> partids = clusterPartIdsMap["cluster1.default"];
        ASSERT_EQ((size_t)2, partids.size());
        ASSERT_EQ((hashid_t)0, partids[0]);
        ASSERT_EQ((hashid_t)1, partids[1]);
    }
    {
        vector<pair<hashid_t, hashid_t> > partids;
        bool ret = resultClusterClause.getClusterPartIds(string("cluster1.default"), partids);
        ASSERT_TRUE(ret);
        ASSERT_EQ((size_t)2, partids.size());
        ASSERT_EQ((hashid_t)0, partids[0].first);
        ASSERT_EQ((hashid_t)0, partids[0].second);
        ASSERT_EQ((hashid_t)1, partids[1].first);
        ASSERT_EQ((hashid_t)1, partids[1].second);
    }
}

END_HA3_NAMESPACE(common);
