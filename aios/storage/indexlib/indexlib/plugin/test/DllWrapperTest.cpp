#include "indexlib/plugin/DllWrapper.h"

#include <string>

#include "autil/StringUtil.h"
#include "indexlib/test/unittest.h"
using namespace std;
using namespace autil;

namespace indexlib { namespace plugin {

class DllWrapperTest : public INDEXLIB_TESTBASE
{
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

protected:
    string _localShareObjPath;
};

void DllWrapperTest::CaseSetUp() { _localShareObjPath = "libdummy.so"; }

void DllWrapperTest::CaseTearDown() {}

TEST_F(DllWrapperTest, testSimpleProcess)
{
    string localShareObjPath = GET_PRIVATE_TEST_DATA_PATH() + "plugins/libdummy.so";
    DllWrapper dllWrapper(localShareObjPath);
    ASSERT_TRUE(dllWrapper.dlopen());

    void* sym = dllWrapper.dlsym("createFactory");
    ASSERT_TRUE(sym);

    void* noSuchSym = dllWrapper.dlsym("noSuchFun");
    ASSERT_TRUE(!noSuchSym);

    string expect = _localShareObjPath + ": undefined symbol: noSuchFun";
    string actual = dllWrapper.dlerror();
    ASSERT_TRUE(actual.find(expect));

    ASSERT_TRUE(dllWrapper.dlclose());
}

TEST_F(DllWrapperTest, testLoadLocalShareObj)
{
    string localShareObjPath = GET_PRIVATE_TEST_DATA_PATH() + "plugins/libdummy.so";
    DllWrapper dllWrapper(localShareObjPath);

    ASSERT_TRUE(dllWrapper.dlopen());

    void* sym = dllWrapper.dlsym("createFactory");
    ASSERT_TRUE(sym);

    void* noSuchSym = dllWrapper.dlsym("noSuchFun");
    ASSERT_TRUE(!noSuchSym);
    string expect = localShareObjPath + ": undefined symbol: noSuchFun";
    string actual = dllWrapper.dlerror();
    ASSERT_EQ(expect, actual);

    ASSERT_TRUE(dllWrapper.dlclose());
}

TEST_F(DllWrapperTest, testLoadNoSuchFile)
{
    string localShareObjPath = "/no/such/file.so";
    DllWrapper dllWrapper(localShareObjPath);
    ASSERT_TRUE(!dllWrapper.dlopen());
}

TEST_F(DllWrapperTest, testLoadWithCorruptedLocalFile)
{
    string localShareObjPath = GET_PRIVATE_TEST_DATA_PATH() + "plugins/libdummy.so";
    DllWrapper dllWrapper(localShareObjPath);
    ASSERT_TRUE(dllWrapper.dlopen());
}

}} // namespace indexlib::plugin
