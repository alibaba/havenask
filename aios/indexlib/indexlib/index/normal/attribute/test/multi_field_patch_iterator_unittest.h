#ifndef __INDEXLIB_MULTIFIELDPATCHITERATORTEST_H
#define __INDEXLIB_MULTIFIELDPATCHITERATORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/multi_field_patch_iterator.h"

IE_NAMESPACE_BEGIN(index);

class MultiFieldPatchIteratorTest : public INDEXLIB_TESTBASE
{
public:
    MultiFieldPatchIteratorTest();
    ~MultiFieldPatchIteratorTest();

    DECLARE_CLASS_NAME(MultiFieldPatchIteratorTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestCreateIndependentPatchWorkItems();
    void TestIncConsistentWithRealtime();
    void TestIterateWithPackAttributes();

private:
    void CheckNormalField(MultiFieldPatchIterator& iter, 
                   docid_t expectDocId, fieldid_t expectFieldId,
                   int32_t expectPatchValue);
    
    void CheckPackField(MultiFieldPatchIterator& iter,
                        docid_t expectDocId, packattrid_t expectPackId,
                        const std::string& expectPatchValue);
    
private:
    std::string mRootDir;
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MultiFieldPatchIteratorTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(MultiFieldPatchIteratorTest, TestCreateIndependentPatchWorkItems);
INDEXLIB_UNIT_TEST_CASE(MultiFieldPatchIteratorTest, TestIncConsistentWithRealtime);
INDEXLIB_UNIT_TEST_CASE(MultiFieldPatchIteratorTest, TestIterateWithPackAttributes);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MULTIFIELDPATCHITERATORTEST_H
