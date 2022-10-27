#ifndef __INDEXLIB_SINGLEFIELDPATCHITERATORTEST_H
#define __INDEXLIB_SINGLEFIELDPATCHITERATORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/single_field_patch_iterator.h"

IE_NAMESPACE_BEGIN(index);

class SingleFieldPatchIteratorTest : public INDEXLIB_TESTBASE
{
public:
    SingleFieldPatchIteratorTest();
    ~SingleFieldPatchIteratorTest();

    DECLARE_CLASS_NAME(SingleFieldPatchIteratorTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestCreateIndependentPatchWorkItems();
    void TestIncConsistentWithRealtime();
    void TestAttributeSegmentPatchInWorkItems();
    void TestFilterLoadedPatchFileInfos();
    void TestInit();
    void TestNext();
private:
    void CheckIterator(SingleFieldPatchIterator& iter, 
                       docid_t expectDocid, fieldid_t expectFieldId,
                       int32_t expectValue);
    void PreparePatchPartitionData(const std::string& docs,
                                   index_base::PartitionDataPtr& partitionData, 
                                   config::AttributeConfigPtr& attrConfig);
private:
    std::string mRootDir;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SingleFieldPatchIteratorTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(SingleFieldPatchIteratorTest, TestFilterLoadedPatchFileInfos);
INDEXLIB_UNIT_TEST_CASE(SingleFieldPatchIteratorTest, TestCreateIndependentPatchWorkItems);
INDEXLIB_UNIT_TEST_CASE(SingleFieldPatchIteratorTest, TestIncConsistentWithRealtime);
INDEXLIB_UNIT_TEST_CASE(SingleFieldPatchIteratorTest, TestAttributeSegmentPatchInWorkItems);
INDEXLIB_UNIT_TEST_CASE(SingleFieldPatchIteratorTest, TestInit);
INDEXLIB_UNIT_TEST_CASE(SingleFieldPatchIteratorTest, TestNext);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SINGLEFIELDPATCHITERATORTEST_H
