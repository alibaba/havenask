#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/FieldMapTermQueryExecutor.h>
#include <ha3_sdk/testlib/index/FakeBufferedPostingIterator.h>
#include <autil/StringUtil.h>

using namespace std;

IE_NAMESPACE_USE(index);
BEGIN_HA3_NAMESPACE(search);

class FieldMapTermQueryExecutorTest : public TESTBASE {
public:
    FieldMapTermQueryExecutorTest();
    ~FieldMapTermQueryExecutorTest();
public:
    void setUp();
    void tearDown();
protected:
    void innerTestSeek(const std::string &docIdStr,
                       const std::string &fieldMapStr, 
                       fieldmap_t targetFieldMap,
                       FieldMatchOperatorType operatorType,
                       const std::string &expectDocIds);
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, FieldMapTermQueryExecutorTest);


FieldMapTermQueryExecutorTest::FieldMapTermQueryExecutorTest() { 
}

FieldMapTermQueryExecutorTest::~FieldMapTermQueryExecutorTest() { 
}

void FieldMapTermQueryExecutorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void FieldMapTermQueryExecutorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(FieldMapTermQueryExecutorTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string docIdStr = "1,2,3,4,5,6,7,8,9";
    string fieldMapStr = "1,2,3,4,5,6,7,8,9";
    {
        fieldmap_t targetFieldMap = 0x03;
        FieldMatchOperatorType operatorType = FM_AND;
        string expectDocIds = "3,7";
        innerTestSeek(docIdStr, fieldMapStr, targetFieldMap, operatorType, expectDocIds);
    }
    {
        fieldmap_t targetFieldMap = 0x03;
        FieldMatchOperatorType operatorType = FM_OR;
        string expectDocIds = "1,2,3,5,6,7,9";
        innerTestSeek(docIdStr, fieldMapStr, targetFieldMap, operatorType, expectDocIds);
    }
}

TEST_F(FieldMapTermQueryExecutorTest, testAndOperatorProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string docIdStr = "1,2,3,4";
    string fieldMapStr = "1,1,1,1";
    {
        fieldmap_t targetFieldMap = 0x01;
        FieldMatchOperatorType operatorType = FM_AND;
        string expectDocIds = "1,2,3,4";
        innerTestSeek(docIdStr, fieldMapStr, targetFieldMap, operatorType, expectDocIds);
    }
    {
        fieldmap_t targetFieldMap = 0x02;
        FieldMatchOperatorType operatorType = FM_AND;
        string expectDocIds = "";
        innerTestSeek(docIdStr, fieldMapStr, targetFieldMap, operatorType, expectDocIds);
    }
}

TEST_F(FieldMapTermQueryExecutorTest, testOrOperatorProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string docIdStr = "1,2,3,4";
    string fieldMapStr = "2,2,2,2";
    {
        fieldmap_t targetFieldMap = 0x02;
        FieldMatchOperatorType operatorType = FM_OR;
        string expectDocIds = "1,2,3,4";
        innerTestSeek(docIdStr, fieldMapStr, targetFieldMap, operatorType, expectDocIds);
    }
    {
        fieldmap_t targetFieldMap = 0x01;
        FieldMatchOperatorType operatorType = FM_OR;
        string expectDocIds = "";
        innerTestSeek(docIdStr, fieldMapStr, targetFieldMap, operatorType, expectDocIds);
    }
}

void FieldMapTermQueryExecutorTest::innerTestSeek(
        const string &docIdStr, const string &fieldMapStr, 
        fieldmap_t targetFieldMap, FieldMatchOperatorType operatorType,
        const string &expectDocIds)
{
    FakeBufferedPostingIterator *iter = new FakeBufferedPostingIterator(
            EXPACK_OPTION_FLAG_ALL, NULL);
    iter->init(docIdStr, fieldMapStr, 1);
    FieldMapTermQueryExecutor executor(iter, common::Term(), targetFieldMap, operatorType);
    docid_t docId = 0;
    vector<docid_t> expectDocs;
    autil::StringUtil::fromString<docid_t>(expectDocIds, expectDocs, ",");
    for (size_t i = 0; i < expectDocs.size(); ++i) {
        docId = executor.legacySeek(docId);
        ASSERT_EQ(expectDocs[i], docId);
        ++docId;
    }
    ASSERT_EQ(END_DOCID, executor.legacySeek(docId));
    delete iter;
}

END_HA3_NAMESPACE(search);

