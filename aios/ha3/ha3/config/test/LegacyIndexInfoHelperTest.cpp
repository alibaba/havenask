#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/util/IndexInfos.h>
#include <ha3/config/LegacyIndexInfoHelper.h>

using namespace suez::turing;
BEGIN_HA3_NAMESPACE(config);

class LegacyIndexInfoHelperTest : public TESTBASE {
public:
    LegacyIndexInfoHelperTest();
    ~LegacyIndexInfoHelperTest();
public:
    void setUp();
    void tearDown();
protected:
    IndexInfos *_indexInfos;
    LegacyIndexInfoHelper *_indexInfoHelper;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(config, LegacyIndexInfoHelperTest);


LegacyIndexInfoHelperTest::LegacyIndexInfoHelperTest() { 
    _indexInfos = NULL;
    _indexInfoHelper = NULL;
}

LegacyIndexInfoHelperTest::~LegacyIndexInfoHelperTest() { 
}

void LegacyIndexInfoHelperTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    _indexInfos = new IndexInfos();
    IndexInfo *indexInfo = NULL;

    indexInfo = new IndexInfo();
    indexInfo->setIndexName("default");
    indexInfo->addField("title", 1000);
    indexInfo->addField("body", 100);
    indexInfo->addField("description", 100);
    _indexInfos->addIndexInfo(indexInfo);

    indexInfo = new IndexInfo();
    indexInfo->setIndexName("title");
    indexInfo->addField("title", 1000);
    _indexInfos->addIndexInfo(indexInfo);

    _indexInfoHelper = new LegacyIndexInfoHelper;
    _indexInfoHelper->setIndexInfos(_indexInfos);
}

void LegacyIndexInfoHelperTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    delete _indexInfoHelper;
    _indexInfoHelper = NULL;
    delete _indexInfos;
    _indexInfos = NULL;
}

TEST_F(LegacyIndexInfoHelperTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");
    ASSERT_EQ((indexid_t)0, _indexInfoHelper->getIndexId("default"));
    ASSERT_EQ(0, _indexInfoHelper->getFieldPosition("default", "title"));
    ASSERT_EQ(1, _indexInfoHelper->getFieldPosition("default", "body"));
}

TEST_F(LegacyIndexInfoHelperTest, testTextIndexInfo) { 
    HA3_LOG(DEBUG, "Begin Test!");

    //"default" index
    ASSERT_EQ((indexid_t)0,
                         _indexInfoHelper->getIndexId("default"));
    ASSERT_EQ(0, _indexInfoHelper->getFieldPosition("default", "title"));
    ASSERT_EQ(1, _indexInfoHelper->getFieldPosition("default", "body"));
    ASSERT_EQ(2, _indexInfoHelper->getFieldPosition("default", "description"));
    ASSERT_EQ(-1, _indexInfoHelper->getFieldPosition("default", "not_such_field"));

    //"title" index
    ASSERT_EQ((indexid_t)1,
                         _indexInfoHelper->getIndexId("title"));
    ASSERT_EQ(0, _indexInfoHelper->getFieldPosition("title", "title"));
}

TEST_F(LegacyIndexInfoHelperTest, testInvalidIndexName) {
    HA3_LOG(DEBUG, "Begin Test!");
    ASSERT_EQ(INVALID_INDEXID, 
                         _indexInfoHelper->getIndexId("NoSuchIndex"));
}

TEST_F(LegacyIndexInfoHelperTest, testInvalidFieldBitName) {
    HA3_LOG(DEBUG, "Begin Test!");
    ASSERT_EQ(-1, _indexInfoHelper->getFieldPosition("default", "NoSuchFieldName"));
}

END_HA3_NAMESPACE(config);

