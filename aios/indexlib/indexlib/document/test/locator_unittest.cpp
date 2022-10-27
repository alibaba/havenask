#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/document/locator.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(document);

class LocatorTest : public INDEXLIB_TESTBASE
{
public:
    LocatorTest()
    {
        mRootDir = GET_TEST_DATA_PATH();
    }

    ~LocatorTest()
    {
    }

    DECLARE_CLASS_NAME(LocatorTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForLocator()
    {
        string stringLocator = "abcdefghijklmn";
        Locator locator2(stringLocator);
        INDEXLIB_TEST_EQUAL(ConstString(stringLocator), locator2.GetLocator());
    }

    void TestCaseForSetAndGet()
    {
        Locator locator;   
        string stringLocator = "123456789";
        locator.SetLocator(stringLocator);
        INDEXLIB_TEST_EQUAL(ConstString(stringLocator), locator.GetLocator());
    }

    void TestCaseForOperator()
    {
        string stringLocator = "123456789";
        Locator locator4(stringLocator);
        Locator locator5;
        Locator locator6;
        locator5.SetLocator(stringLocator);
        locator6 = locator4;
        INDEXLIB_TEST_TRUE(locator4 == locator5);
        INDEXLIB_TEST_TRUE(locator5 == locator6);
    }
private:
    string mRootDir;
};

INDEXLIB_UNIT_TEST_CASE(LocatorTest, TestCaseForLocator);
INDEXLIB_UNIT_TEST_CASE(LocatorTest, TestCaseForSetAndGet);
INDEXLIB_UNIT_TEST_CASE(LocatorTest, TestCaseForOperator);

IE_NAMESPACE_END(document);
