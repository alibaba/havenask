#ifndef __AITHETA_QUERY_PARSE_TEST_H
#define __AITHETA_QUERY_PARSE_TEST_H

#include "aitheta_indexer/plugins/aitheta/test/aitheta_test_base.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class AithetaQueryParseTest : public AithetaTestBase {
 public:
    AithetaQueryParseTest() = default;
    ~AithetaQueryParseTest() = default;

 public:
    DECLARE_CLASS_NAME(AithetaQueryParseTest);

 public:
    void TestParseQuery();
    void TestPaseQueryWithScoreFilter();
};

INDEXLIB_UNIT_TEST_CASE(AithetaQueryParseTest, TestParseQuery);
INDEXLIB_UNIT_TEST_CASE(AithetaQueryParseTest, TestPaseQueryWithScoreFilter);

IE_NAMESPACE_END(aitheta_plugin);
#endif  // __AITHETA_QUERY_PARSE_TEST_H
