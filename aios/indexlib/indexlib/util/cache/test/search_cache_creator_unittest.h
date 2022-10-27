#ifndef __INDEXLIB_SEARCHCACHECREATORTEST_H
#define __INDEXLIB_SEARCHCACHECREATORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/cache/search_cache_creator.h"

IE_NAMESPACE_BEGIN(util);

class SearchCacheCreatorTest : public INDEXLIB_TESTBASE
{
public:
    SearchCacheCreatorTest();
    ~SearchCacheCreatorTest();

    DECLARE_CLASS_NAME(SearchCacheCreatorTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestEmptyParam();
    void TestInvalidParam();
    void TestValidParam();
private:
    SearchCache* Create(const std::string& param);
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SearchCacheCreatorTest, TestEmptyParam);
INDEXLIB_UNIT_TEST_CASE(SearchCacheCreatorTest, TestInvalidParam);
INDEXLIB_UNIT_TEST_CASE(SearchCacheCreatorTest, TestValidParam);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_SEARCHCACHECREATORTEST_H
