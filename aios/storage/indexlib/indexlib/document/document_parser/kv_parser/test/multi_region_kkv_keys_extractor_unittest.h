#ifndef __INDEXLIB_MULTIREGIONKKVKEYSEXTRACTORTEST_H
#define __INDEXLIB_MULTIREGIONKKVKEYSEXTRACTORTEST_H

#include "indexlib/common_define.h"
#include "indexlib/document/document_parser/kv_parser/multi_region_kkv_keys_extractor.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace document {

class MultiRegionKkvKeysExtractorTest : public INDEXLIB_TESTBASE
{
public:
    MultiRegionKkvKeysExtractorTest();
    ~MultiRegionKkvKeysExtractorTest();

    DECLARE_CLASS_NAME(MultiRegionKkvKeysExtractorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MultiRegionKkvKeysExtractorTest, TestSimpleProcess);
}} // namespace indexlib::document

#endif //__INDEXLIB_MULTIREGIONKKVKEYSEXTRACTORTEST_H
