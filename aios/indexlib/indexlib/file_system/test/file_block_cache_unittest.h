#ifndef __INDEXLIB_FILEBLOCKCACHETEST_H
#define __INDEXLIB_FILEBLOCKCACHETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/file_block_cache.h"

IE_NAMESPACE_BEGIN(file_system);

class FileBlockCacheTest : public INDEXLIB_TESTBASE
{
public:
    FileBlockCacheTest();
    ~FileBlockCacheTest();

    DECLARE_CLASS_NAME(FileBlockCacheTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestInitWithConfigStr();
    void TestMemoryQuotaController();

private:
    bool CheckInitWithConfigStr(const std::string& paramStr,
                                int64_t expectCacheSize = -1,
                                bool useClockCache = false,
                                int expectedShardBits = 6);
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(FileBlockCacheTest, TestInitWithConfigStr);
INDEXLIB_UNIT_TEST_CASE(FileBlockCacheTest, TestMemoryQuotaController);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FILEBLOCKCACHETEST_H
