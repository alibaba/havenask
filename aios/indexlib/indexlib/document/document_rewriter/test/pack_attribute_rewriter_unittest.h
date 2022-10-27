#ifndef __INDEXLIB_PACKATTRIBUTEREWRITERTEST_H
#define __INDEXLIB_PACKATTRIBUTEREWRITERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/document/document_rewriter/pack_attribute_rewriter.h"

IE_NAMESPACE_BEGIN(document);

class PackAttributeRewriterTest : public INDEXLIB_TESTBASE
{
public:
    PackAttributeRewriterTest();
    ~PackAttributeRewriterTest();

    DECLARE_CLASS_NAME(PackAttributeRewriterTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestRewriteWithSubDoc();
    void TestRewriteCheckPackField();

private:
    config::IndexPartitionSchemaPtr CreateSchema(
        bool mainSchemaHasPack, bool subSchemaHasPack);
    
    void InnerTestRewrite(
        bool mainSchemaHasPack, bool subSchemaHasPack);
        
private:
    std::string mDocStr;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PackAttributeRewriterTest, TestRewriteWithSubDoc);
INDEXLIB_UNIT_TEST_CASE(PackAttributeRewriterTest, TestRewriteCheckPackField);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_PACKATTRIBUTEREWRITERTEST_H
