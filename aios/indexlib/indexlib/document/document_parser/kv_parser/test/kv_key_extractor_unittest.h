#ifndef __INDEXLIB_KVKEYEXTRACTORTEST_H
#define __INDEXLIB_KVKEYEXTRACTORTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/document/document_parser/kv_parser/multi_region_kv_key_extractor.h"

IE_NAMESPACE_BEGIN(document);

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

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_KVKEYEXTRACTORTEST_H
