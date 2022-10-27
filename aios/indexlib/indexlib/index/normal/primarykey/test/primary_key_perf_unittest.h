#ifndef __INDEXLIB_PRIMARYKEYPERFTEST_H
#define __INDEXLIB_PRIMARYKEYPERFTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/config/impl/primary_key_index_config_impl.h"
#include "indexlib/file_system/directory.h"
#include "autil/mem_pool/Pool.h"

IE_NAMESPACE_BEGIN(index);

class PrimaryKeyPerfTest : public INDEXLIB_TESTBASE
{
public:
    PrimaryKeyPerfTest();
    ~PrimaryKeyPerfTest();

    DECLARE_CLASS_NAME(PrimaryKeyPerfTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestDumpPerf();
    void TestLoadPerf();

private:
    void DoTestDumpPerf(const config::IndexConfigPtr& pkConfig);
private:
    file_system::DirectoryPtr mRootDir;
    std::tr1::shared_ptr<autil::mem_pool::Pool> mPool;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PrimaryKeyPerfTest, TestDumpPerf);
// INDEXLIB_UNIT_TEST_CASE(PrimaryKeyPerfTest, TestLoadPerf);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PRIMARYKEYPERFTEST_H
