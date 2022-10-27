#ifndef __INDEXLIB_TERMMETALOADERTEST_H
#define __INDEXLIB_TERMMETALOADERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/accessor/term_meta.h"
#include "indexlib/file_system/directory.h"

IE_NAMESPACE_BEGIN(index);

// seprate from common/term_meta_unittest.h
class TermMetaLoaderTest : public INDEXLIB_TESTBASE {
public:
    TermMetaLoaderTest();
    ~TermMetaLoaderTest();
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestStoreSize();
    void TestDump();
    void TestLoad();
    static void DumpTermMeta(const index::TermMeta& termMeta,
                             const file_system::DirectoryPtr& directory,
                             const std::string& fileName);
private:
    void CheckDump(index::TermMeta& termMeta);
private:
    std::string mRootPath;
    file_system::DirectoryPtr mRootDirectory;
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TermMetaLoaderTest, TestStoreSize);
INDEXLIB_UNIT_TEST_CASE(TermMetaLoaderTest, TestDump);
INDEXLIB_UNIT_TEST_CASE(TermMetaLoaderTest, TestLoad);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TERMMETALOADERTEST_H
