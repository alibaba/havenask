#include "indexlib/common_define.h"
#include "indexlib/document/locator.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/test/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib::file_system;

namespace indexlib { namespace document {

class LocatorTest : public INDEXLIB_TESTBASE
{
public:
    LocatorTest() {}

    ~LocatorTest() {}

    DECLARE_CLASS_NAME(LocatorTest);
    void CaseSetUp() override {}

    void CaseTearDown() override {}

    void TestCaseForLocator()
    {
        string stringLocator = "abcdefghijklmn";
        Locator locator2(stringLocator);
        INDEXLIB_TEST_EQUAL(StringView(stringLocator), locator2.GetLocator());
    }

    void TestCaseForSetAndGet()
    {
        Locator locator;
        string stringLocator = "123456789";
        locator.SetLocator(stringLocator);
        INDEXLIB_TEST_EQUAL(StringView(stringLocator), locator.GetLocator());
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
};

INDEXLIB_UNIT_TEST_CASE(LocatorTest, TestCaseForLocator);
INDEXLIB_UNIT_TEST_CASE(LocatorTest, TestCaseForSetAndGet);
INDEXLIB_UNIT_TEST_CASE(LocatorTest, TestCaseForOperator);
}} // namespace indexlib::document
