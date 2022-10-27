#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/GlobalIdentifier.h>

using namespace std;

BEGIN_HA3_NAMESPACE(common);

class GlobalIdentifierTest : public TESTBASE {
public:
    GlobalIdentifierTest();
    ~GlobalIdentifierTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, GlobalIdentifierTest);

GlobalIdentifierTest::GlobalIdentifierTest() {
}

GlobalIdentifierTest::~GlobalIdentifierTest() {
}

void GlobalIdentifierTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void GlobalIdentifierTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(GlobalIdentifierTest, testConstruct) {
    GlobalIdentifier identifier;
    ASSERT_EQ((docid_t)0, identifier.getDocId());
    ASSERT_EQ((hashid_t)0, identifier.getHashId());
    ASSERT_EQ((clusterid_t)0, identifier.getClusterId());
    ASSERT_EQ((uint32_t)0, identifier.getIp());

    ASSERT_EQ((primarykey_t)0, identifier.getPrimaryKey());
    ASSERT_EQ((versionid_t)0, identifier.getIndexVersion());
    ASSERT_EQ((FullIndexVersion)0, identifier.getFullIndexVersion());
    ASSERT_EQ((uint32_t)-1, identifier.getPosition());
}

TEST_F(GlobalIdentifierTest, testSetInfo) {
    GlobalIdentifier identifier;
    identifier.setDocId((docid_t)1);
    identifier.setHashId((hashid_t)2);
    identifier.setClusterId((clusterid_t)3);
    identifier.setIp((uint32_t)-2);

    ASSERT_EQ((docid_t)1, identifier.getDocId());
    ASSERT_EQ((hashid_t)2, identifier.getHashId());
    ASSERT_EQ((clusterid_t)3, identifier.getClusterId());
    ASSERT_EQ((uint32_t)-2, identifier.getIp());
}

TEST_F(GlobalIdentifierTest, testSerializeAndDeserialize) {
    GlobalIdentifier identifier;
    identifier.setDocId((docid_t)1);
    identifier.setHashId((hashid_t)2);
    identifier.setClusterId((clusterid_t)3);
    identifier.setPrimaryKey((primarykey_t)100);
    identifier.setIndexVersion(versionid_t(1));
    identifier.setFullIndexVersion((FullIndexVersion)1);
    identifier.setIp((uint32_t)-3);

    ASSERT_EQ((docid_t)1, identifier.getDocId());
    ASSERT_EQ((hashid_t)2, identifier.getHashId());
    ASSERT_EQ((clusterid_t)3, identifier.getClusterId());
    ASSERT_EQ((primarykey_t)100, identifier.getPrimaryKey());
    ASSERT_EQ(versionid_t(1), identifier.getIndexVersion());
    ASSERT_EQ(FullIndexVersion(1), identifier.getFullIndexVersion());
    ASSERT_EQ((uint32_t)-3, identifier.getIp());

    autil::DataBuffer dataBuffer;
    identifier.serialize(dataBuffer);

    GlobalIdentifier identifier2;
    identifier2.deserialize(dataBuffer);

    ASSERT_EQ((docid_t)1, identifier2.getDocId());
    ASSERT_EQ((hashid_t)2, identifier2.getHashId());
    ASSERT_EQ((clusterid_t)3, identifier2.getClusterId());
    ASSERT_EQ((primarykey_t)100, identifier2.getPrimaryKey());
    ASSERT_EQ(versionid_t(1), identifier2.getIndexVersion());
    ASSERT_EQ(FullIndexVersion(1), identifier2.getFullIndexVersion());
    ASSERT_EQ((uint32_t)-3, identifier2.getIp());
}

TEST_F(GlobalIdentifierTest, testOperator) {
    GlobalIdentifier identifier;
    identifier.setDocId((docid_t)1);
    identifier.setHashId((hashid_t)2);
    identifier.setClusterId((clusterid_t)3);
    identifier.setPrimaryKey((primarykey_t)100);

    GlobalIdentifier identifier2;
    identifier2.setDocId((docid_t)2);
    identifier2.setHashId((hashid_t)2);
    identifier2.setClusterId((clusterid_t)3);
    identifier2.setPrimaryKey((primarykey_t)100);

    GlobalIdentifier identifier3;
    identifier3.setDocId((docid_t)2);
    identifier3.setHashId((hashid_t)2);
    identifier3.setClusterId((clusterid_t)3);
    identifier3.setPrimaryKey((primarykey_t)100);
    identifier3.setIndexVersion((versionid_t)2);

    GlobalIdentifier identifier4;
    identifier4.setDocId((docid_t)2);
    identifier4.setHashId((hashid_t)1);
    identifier4.setClusterId((clusterid_t)3);
    identifier4.setPrimaryKey((primarykey_t)100);

    GlobalIdentifier identifier5;
    identifier5.setDocId((docid_t)2);
    identifier5.setHashId((hashid_t)1);
    identifier5.setClusterId((clusterid_t)3);
    identifier5.setPrimaryKey((primarykey_t)101);


    ASSERT_TRUE(identifier < identifier2);
    ASSERT_TRUE(identifier2 > identifier);
    ASSERT_TRUE(identifier2 < identifier3);
    ASSERT_TRUE(identifier3 > identifier2);
    ASSERT_TRUE(identifier < identifier4);
    ASSERT_TRUE(identifier4 < identifier5);
}

TEST_F(GlobalIdentifierTest, testSetAndGetFullIndexVersion) {
    GlobalIdentifier identifier;
    ASSERT_EQ(uint32_t(0), identifier.getFullIndexVersion());
    identifier.setFullIndexVersion(5);
    ASSERT_EQ(uint32_t(5), identifier.getFullIndexVersion());
}

END_HA3_NAMESPACE(common);

