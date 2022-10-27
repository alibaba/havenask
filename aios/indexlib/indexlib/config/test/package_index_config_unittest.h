#ifndef __INDEXLIB_PACKAGEINDEXCONFIGTEST_H
#define __INDEXLIB_PACKAGEINDEXCONFIGTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/config/impl/package_index_config_impl.h"

IE_NAMESPACE_BEGIN(config);

class PackageIndexConfigTest : public INDEXLIB_TESTBASE {
public:
    PackageIndexConfigTest();
    ~PackageIndexConfigTest();
public:
    void SetUp();
    void TearDown();
    void TestJsonize();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PackageIndexConfigTest, TestJsonize);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_PACKAGEINDEXCONFIGTEST_H
