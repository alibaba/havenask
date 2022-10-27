#ifndef __INDEXLIB_SUBDOCMODIFIERTEST_H
#define __INDEXLIB_SUBDOCMODIFIERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/modifier/sub_doc_modifier.h"
#include "indexlib/partition/test/mock_partition_modifier.h"
#include "indexlib/index/normal/attribute/accessor/join_docid_attribute_reader.h"

IE_NAMESPACE_BEGIN(partition);

class SubDocModifierTest : public INDEXLIB_TESTBASE
{
public:
    SubDocModifierTest();
    ~SubDocModifierTest();

    DECLARE_CLASS_NAME(SubDocModifierTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestRemoveDupSubDocument();
    void TestNeedUpdate();
    void TestIsDirty();
    void TestValidateSubDocsIfMainDocIdInvalid();
    void TestValidateSubDocs();
    void TestGetSubDocIdRange();
//    void TestUpdateDocumentWithNoField();
    void TestInit();

private:
    SubDocModifierPtr CreateModifier();
    index::JoinDocidAttributeReaderPtr CreateJoinDocidAttributeReader(
            std::string mainInc, std::string subInc);

    bool CheckGetSubDocIdRange(std::string mainJoinStr, std::string subJoinStr,
                               docid_t docid, docid_t expectBegin, docid_t expectEnd);

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionSchemaPtr mSubSchema;
    MockPartitionModifierPtr mMainModifier;
    MockPartitionModifierPtr mSubModifier;
    SubDocModifierPtr mModifier;
    document::NormalDocumentPtr mDoc;
    document::NormalDocumentPtr mSubDoc;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SubDocModifierTest, TestRemoveDupSubDocument);
INDEXLIB_UNIT_TEST_CASE(SubDocModifierTest, TestNeedUpdate);
INDEXLIB_UNIT_TEST_CASE(SubDocModifierTest, TestIsDirty);
INDEXLIB_UNIT_TEST_CASE(SubDocModifierTest, TestValidateSubDocsIfMainDocIdInvalid);
INDEXLIB_UNIT_TEST_CASE(SubDocModifierTest, TestValidateSubDocs);
INDEXLIB_UNIT_TEST_CASE(SubDocModifierTest, TestGetSubDocIdRange);
//INDEXLIB_UNIT_TEST_CASE(SubDocModifierTest, TestUpdateDocumentWithNoField);
INDEXLIB_UNIT_TEST_CASE(SubDocModifierTest, TestInit);


IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_SUBDOCMODIFIERTEST_H
