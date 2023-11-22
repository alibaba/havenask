#pragma once

#include "indexlib/common_define.h"
#include "indexlib/document/document_parser/kv_parser/multi_region_kv_key_extractor.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace document {

class KvKeyExtractorTest : public INDEXLIB_TESTBASE
{
public:
    KvKeyExtractorTest();
    ~KvKeyExtractorTest();

    DECLARE_CLASS_NAME(KvKeyExtractorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestHashKey();
    void TestHashKeyWithMurmurHash();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(KvKeyExtractorTest, TestHashKey);
INDEXLIB_UNIT_TEST_CASE(KvKeyExtractorTest, TestHashKeyWithMurmurHash);
}} // namespace indexlib::document
