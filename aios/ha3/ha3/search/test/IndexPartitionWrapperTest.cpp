#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/IndexPartitionWrapper.h>
#include <ha3_sdk/testlib/index/FakeIndexPartition.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3/test/test.h>

using namespace std;
using namespace suez::turing;
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(index);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(testlib);

BEGIN_HA3_NAMESPACE(search);

class IndexPartitionWrapperTest : public TESTBASE {
public:
    IndexPartitionWrapperTest();
    ~IndexPartitionWrapperTest();
public:
    void setUp();
    void tearDown();
protected:
    IE_NAMESPACE(testlib)::FakeIndexPartition *fakeIndexPart;
    IE_NAMESPACE(partition)::IndexPartitionPtr mainIndexPart;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, IndexPartitionWrapperTest);


IndexPartitionWrapperTest::IndexPartitionWrapperTest() {
    fakeIndexPart = NULL;
}

IndexPartitionWrapperTest::~IndexPartitionWrapperTest() {
}

void IndexPartitionWrapperTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    fakeIndexPart = new FakeIndexPartition;
    fakeIndexPart->setSchema(TEST_DATA_PATH, "searcher_test_schema.json");
    mainIndexPart.reset(fakeIndexPart);
}

void IndexPartitionWrapperTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
    mainIndexPart.reset();
}

TEST_F(IndexPartitionWrapperTest, testInit) {
    HA3_LOG(DEBUG, "Begin Test!");
    {
        IndexPartitionWrapper indexPartWrapper;
        ASSERT_TRUE(indexPartWrapper.init(mainIndexPart));
    }

}

TEST_F(IndexPartitionWrapperTest, testIsSubField) {
    IndexPartitionWrapper indexPartWrapper;
    ASSERT_TRUE(indexPartWrapper.isSubField(fakeIndexPart->GetSchema(), "sub_price"));
    ASSERT_TRUE(!indexPartWrapper.isSubField(fakeIndexPart->GetSchema(), "price"));
    ASSERT_TRUE(!indexPartWrapper.isSubField(fakeIndexPart->GetSchema(), "invalid_attribute"));
}

END_HA3_NAMESPACE(search);
