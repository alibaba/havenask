#include "sql/resource/HashFunctionCacheR.h"

#include <iosfwd>
#include <memory>

#include "autil/HashFuncFactory.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "navi/tester/NaviResourceHelper.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil;

namespace sql {

class HashFunctionCacheRTest : public TESTBASE {
public:
    void setUp() override;
    void tearDown() override;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(resource, HashFunctionCacheRTest);

void HashFunctionCacheRTest::setUp() {}

void HashFunctionCacheRTest::tearDown() {}

TEST_F(HashFunctionCacheRTest, testCreateResource) {
    navi::NaviResourceHelper naviRes;
    HashFunctionCacheR *hashFuncCacheR = nullptr;
    ASSERT_TRUE(naviRes.getOrCreateRes(hashFuncCacheR));
}

TEST_F(HashFunctionCacheRTest, testPutGet) {
    HashFunctionCacheR hashFuncCache;
    auto hashFunc = hashFuncCache.getHashFunc(0);
    ASSERT_TRUE(hashFunc == nullptr);
    auto rawHashFunc = HashFuncFactory::createHashFunc("HASH");
    hashFuncCache.putHashFunc(0, rawHashFunc);
    hashFunc = hashFuncCache.getHashFunc(0);
    ASSERT_TRUE(hashFunc != nullptr);
    ASSERT_TRUE(hashFunc.get() == rawHashFunc.get());
}

} // namespace sql
