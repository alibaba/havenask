#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/DocIdentifier.h>

using namespace std;

BEGIN_HA3_NAMESPACE(common);

class DocIdentifierTest : public TESTBASE {
public:
    DocIdentifierTest();
    ~DocIdentifierTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, DocIdentifierTest);


DocIdentifierTest::DocIdentifierTest() { 
}

DocIdentifierTest::~DocIdentifierTest() { 
}

void DocIdentifierTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void DocIdentifierTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(DocIdentifierTest, testDefaultValue) { 
    HA3_LOG(DEBUG, "Begin Test!");

    DocIdentifier docIdentifier;
    ASSERT_EQ((docid_t)0, docIdentifier.getDocId());
    ASSERT_EQ((hashid_t)0, docIdentifier.getHashId());
    ASSERT_EQ((clusterid_t)0, docIdentifier.getClusterId());
    ASSERT_EQ((uint64_t)0, docIdentifier.getUint64Identifier());
}

TEST_F(DocIdentifierTest, testSetAndGet) {
    DocIdentifier docIdentifier;
    docIdentifier.setDocId(3);
    docIdentifier.setHashId(2);
    docIdentifier.setClusterId(5);

    ASSERT_EQ((docid_t)3, docIdentifier.getDocId());
    ASSERT_EQ((hashid_t)2, docIdentifier.getHashId());
    ASSERT_EQ((clusterid_t)5, docIdentifier.getClusterId());
    ASSERT_EQ((uint64_t)0x2050000000003, docIdentifier.getUint64Identifier());

    // set again
    docIdentifier.setDocId(8);
    docIdentifier.setHashId(7);
    docIdentifier.setClusterId(3);
    ASSERT_EQ((docid_t)8, docIdentifier.getDocId());
    ASSERT_EQ((hashid_t)7, docIdentifier.getHashId());
    ASSERT_EQ((clusterid_t)3, docIdentifier.getClusterId());
    ASSERT_EQ((uint64_t)0x7030000000008, docIdentifier.getUint64Identifier());

    docIdentifier.setDocFlag(7);
    ASSERT_TRUE(docIdentifier.isDocFlag(7));
    ASSERT_TRUE(!docIdentifier.isDocFlag(6));
    ASSERT_TRUE(!docIdentifier.isDocFlag(0));
    ASSERT_EQ((uint64_t)0x7038000000008, docIdentifier.getUint64Identifier());

    // boundary
    docIdentifier.setUint64Identifier(0);
    docIdentifier.setDocFlag(31);
    ASSERT_EQ((uint64_t)0x8000000000000000, docIdentifier.getUint64Identifier());
}

TEST_F(DocIdentifierTest, testLessThan) {
    DocIdentifier docIdentifier1;
    DocIdentifier docIdentifier2;
    
    ASSERT_TRUE(!(docIdentifier1 < docIdentifier2));
    ASSERT_TRUE(!(docIdentifier1 > docIdentifier2));

    docIdentifier1.setDocId(3);
    ASSERT_TRUE(docIdentifier1 > docIdentifier2);
    docIdentifier2.setDocId(5);
    ASSERT_TRUE(docIdentifier1 < docIdentifier2);

    docIdentifier2.setDocId(3);
    docIdentifier2.setHashId(7);
    ASSERT_TRUE(docIdentifier1 < docIdentifier2);

    docIdentifier1.setDocId(4);
    docIdentifier1.setHashId(2);
    ASSERT_TRUE(docIdentifier1 > docIdentifier2);
}

END_HA3_NAMESPACE(common);

