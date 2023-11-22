#pragma once

#include "indexlib/common_define.h"
#include "indexlib/document/document_factory_wrapper.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace document {

class DocumentFactoryWrapperTest : public INDEXLIB_TESTBASE
{
public:
    DocumentFactoryWrapperTest();
    ~DocumentFactoryWrapperTest();

    DECLARE_CLASS_NAME(DocumentFactoryWrapperTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestInitBuiltInFactory();
    void TestInitPluginInFactory();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DocumentFactoryWrapperTest, TestInitBuiltInFactory);
INDEXLIB_UNIT_TEST_CASE(DocumentFactoryWrapperTest, TestInitPluginInFactory);
}} // namespace indexlib::document
