#ifndef __INDEXLIB_INPLACEMODIFIERTEST_H
#define __INDEXLIB_INPLACEMODIFIERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/modifier/inplace_modifier.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"

IE_NAMESPACE_BEGIN(partition);

class InplaceModifierTest : public INDEXLIB_TESTBASE
{
public:
    InplaceModifierTest();
    ~InplaceModifierTest();

    DECLARE_CLASS_NAME(InplaceModifierTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestUpdateDocument();
    void TestDeleteDocument();
    void TestUpdateDocumentWithoutSchema();

private:
    std::string mRootDir;

private:
    void CheckAttribute(const index::AttributeReader& reader,
                        docid_t docid, int32_t expectValue);

    void CheckDeletionMap(const index::DeletionMapReaderPtr& reader,
                          const std::string& docIdStrs, bool isDeleted);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InplaceModifierTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(InplaceModifierTest, TestUpdateDocument);
INDEXLIB_UNIT_TEST_CASE(InplaceModifierTest, TestDeleteDocument);
INDEXLIB_UNIT_TEST_CASE(InplaceModifierTest, TestUpdateDocumentWithoutSchema);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_INPLACEMODIFIERTEST_H
