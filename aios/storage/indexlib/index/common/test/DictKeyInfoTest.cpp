#include "indexlib/index/common/DictKeyInfo.h"

#include "indexlib/util/testutil/unittest.h"

using namespace std;
namespace indexlib::index {

class DictKeyInfoTest : public INDEXLIB_TESTBASE
{
public:
    DictKeyInfoTest();
    ~DictKeyInfoTest();

    DECLARE_CLASS_NAME(DictKeyInfoTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DictKeyInfoTest, TestSimpleProcess);

AUTIL_LOG_SETUP(indexlib.index, DictKeyInfoTest);

DictKeyInfoTest::DictKeyInfoTest() {}

DictKeyInfoTest::~DictKeyInfoTest() {}

void DictKeyInfoTest::CaseSetUp() {}

void DictKeyInfoTest::CaseTearDown() {}

void DictKeyInfoTest::TestSimpleProcess()
{
    ASSERT_EQ(index::DictKeyInfo(1), index::DictKeyInfo(1, false));
    ASSERT_NE(index::DictKeyInfo(1), index::DictKeyInfo(1, true));

    ASSERT_LT(index::DictKeyInfo(1, false), index::DictKeyInfo(2, false));
    ASSERT_LT(index::DictKeyInfo(1, true), index::DictKeyInfo(2, false));
    ASSERT_GT(index::DictKeyInfo(3, false), index::DictKeyInfo(2, true));
    ASSERT_GT(index::DictKeyInfo(3, true), index::DictKeyInfo(2, true));

    ASSERT_LT(index::DictKeyInfo(1, false), index::DictKeyInfo(1, true));
    ASSERT_GT(index::DictKeyInfo(1, true), index::DictKeyInfo(1, false));

    ASSERT_LT(index::DictKeyInfo(numeric_limits<dictkey_t>::max()), index::DictKeyInfo::NULL_TERM);

    index::DictKeyInfo value(125, true);
    ASSERT_EQ(string("125:true"), value.ToString());

    value.SetIsNull(false);
    ASSERT_EQ(string("125"), value.ToString());

    index::DictKeyInfo newValue;
    newValue.FromString("137:true");
    ASSERT_EQ(index::DictKeyInfo(137, true), newValue);

    newValue.FromString("138");
    ASSERT_EQ(index::DictKeyInfo(138), newValue);
    ASSERT_ANY_THROW(newValue.FromString("138:abc"));
    ASSERT_ANY_THROW(newValue.FromString("138:false"));
    ASSERT_ANY_THROW(newValue.FromString("138:true:1"));
    ASSERT_ANY_THROW(newValue.FromString("abc:true"));
    ASSERT_ANY_THROW(newValue.FromString("true"));
}
} // namespace indexlib::index
