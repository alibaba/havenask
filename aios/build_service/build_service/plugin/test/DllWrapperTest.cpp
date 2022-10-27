#include "build_service/test/unittest.h"
#include "build_service/plugin/DllWrapper.h"
#include <string>
#include <autil/StringUtil.h>
#include "build_service/util/FileUtil.h"

using namespace std;

namespace build_service {
namespace plugin {

class DllWrapperTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    std::string _localShareObjPath;
};


void DllWrapperTest::setUp() {
    _localShareObjPath = "libdummy.so";
}

void DllWrapperTest::tearDown() {
}


TEST_F(DllWrapperTest, testSimpleProcess) {
    BS_LOG(DEBUG, "Begin Test!");
    DllWrapper dllWrapper(_localShareObjPath);
    BS_LOG(ERROR, "LOCAL_BASE_DIR:[%s]", _localShareObjPath.c_str());
    ASSERT_TRUE(dllWrapper.dlopen());

    void *sym = dllWrapper.dlsym("createFactory");
    ASSERT_TRUE(sym);

    void *noSuchSym = dllWrapper.dlsym("noSuchFun");
    ASSERT_TRUE(!noSuchSym);


    string expect = _localShareObjPath + ": undefined symbol: noSuchFun";
    string actual = dllWrapper.dlerror();
    ASSERT_TRUE(actual.find(expect));

    ASSERT_TRUE(dllWrapper.dlclose());
}

TEST_F(DllWrapperTest, testLoadLocalShareObj) {
    BS_LOG(DEBUG, "Begin Test!");

    string localShareObjPath = TEST_DATA_PATH"plugins/libdummy.so";

    DllWrapper dllWrapper(localShareObjPath);

    ASSERT_TRUE(dllWrapper.dlopen());

    void *sym = dllWrapper.dlsym("createFactory");
    ASSERT_TRUE(sym);

    void *noSuchSym = dllWrapper.dlsym("noSuchFun");
    ASSERT_TRUE(!noSuchSym);
    string expect = localShareObjPath + ": undefined symbol: noSuchFun";
    BS_LOG(DEBUG, "before dlerror()");
    string actual = dllWrapper.dlerror();
    BS_LOG(DEBUG, "after dlerror()");
    ASSERT_EQ(expect, actual);

    ASSERT_TRUE(dllWrapper.dlclose());
}

TEST_F(DllWrapperTest, testLoadNoSuchFile) {
    BS_LOG(DEBUG, "Begin Test!");

    string localShareObjPath = "/no/such/file.so";

    DllWrapper dllWrapper(localShareObjPath);
    ASSERT_TRUE(!dllWrapper.dlopen());
}

TEST_F(DllWrapperTest, testLoadWithCorruptedLocalFile) {
    BS_LOG(DEBUG, "Begin Test!");

    DllWrapper dllWrapper(_localShareObjPath);
    ASSERT_TRUE(dllWrapper.dlopen());
}


}
}
