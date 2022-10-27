#include <ha3_sdk/testlib/index/FakeNumberIndexReader.h>
#include <unittest/unittest.h>
#include <ha3/util/NumericLimits.h>
#include <ha3/util/Log.h>
#include <indexlib/common/number_term.h>

using namespace std;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_BEGIN(index);

class FakeNumberIndexReaderTest : public TESTBASE {
public:
    void setUp() {}
    void tearDown() {}
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(index, FakeNumberIndexReaderTest);

TEST_F(FakeNumberIndexReaderTest, testSimpleProcess) {
    HA3_LOG(DEBUG, "Begin Test!");

    FakeNumberIndexReader fakeNumberIdxReader;
    string str1 = "1: 3, 5;";
    string str2 = "2: 1, 2";
    fakeNumberIdxReader.setNumberValuesFromString(str1 + str2);

    Int32Term numberTerm(1, true, 2, true, "dummy");
    PostingIteratorPtr postingItPtr(fakeNumberIdxReader.Lookup(numberTerm, false));

    ASSERT_EQ((docid_t) 1, postingItPtr->SeekDoc(0));
    ASSERT_EQ((docid_t) 2, postingItPtr->SeekDoc(2));
    ASSERT_EQ((docid_t) 3, postingItPtr->SeekDoc(3));
    ASSERT_EQ((docid_t) 5, postingItPtr->SeekDoc(4));
}

TEST_F(FakeNumberIndexReaderTest, testSetNumberValuesFromString) {
    HA3_LOG(DEBUG, "Begin Test!");

    FakeNumberIndexReader fakeNumberIdxReader;
    ASSERT_EQ((size_t)0, fakeNumberIdxReader.size());

    string str1 = "1: 3, 5;";
    string str2 = "2: 1, 2";

    fakeNumberIdxReader.setNumberValuesFromString(str1 + str2);
    FakeNumberIndexReader::NumberValuesPtr numberValuesPtr =
        fakeNumberIdxReader.getNumberValues();

    ASSERT_EQ((size_t)6, fakeNumberIdxReader.size());
    ASSERT_EQ(1, numberValuesPtr->at(3));
    ASSERT_EQ(1, numberValuesPtr->at(5));
    ASSERT_EQ(2, numberValuesPtr->at(1));
    ASSERT_EQ(2, numberValuesPtr->at(2));
    ASSERT_EQ(HA3_NS(util)::NumericLimits<int32_t>::min(), numberValuesPtr->at(0));
    ASSERT_EQ(HA3_NS(util)::NumericLimits<int32_t>::min(), numberValuesPtr->at(4));

    fakeNumberIdxReader.clear();
    ASSERT_EQ((size_t)0, fakeNumberIdxReader.size());
}



IE_NAMESPACE_END(index);
