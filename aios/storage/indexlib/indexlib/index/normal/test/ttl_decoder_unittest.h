#ifndef __INDEXLIB_TTLDECODERTEST_H
#define __INDEXLIB_TTLDECODERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/document/document.h"
#include "indexlib/index/normal/ttl_decoder.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class TtlDecoderTest : public INDEXLIB_TESTBASE
{
public:
    TtlDecoderTest();
    ~TtlDecoderTest();

    DECLARE_CLASS_NAME(TtlDecoderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSetDocumentTTL();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TtlDecoderTest, TestSetDocumentTTL);
}} // namespace indexlib::index

#endif //__INDEXLIB_TTLDECODERTEST_H
