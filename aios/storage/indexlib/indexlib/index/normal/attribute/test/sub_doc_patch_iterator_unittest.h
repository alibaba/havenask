#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/sub_doc_patch_iterator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class SubDocPatchIteratorTest : public INDEXLIB_TESTBASE
{
public:
    SubDocPatchIteratorTest();
    ~SubDocPatchIteratorTest();

    DECLARE_CLASS_NAME(SubDocPatchIteratorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    void CheckIter(SubDocPatchIterator& iter, docid_t expectDocid, fieldid_t expectFieldId, int32_t expectPatchValue,
                   bool isSub);

private:
    std::string mRootDir;
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionSchemaPtr mSubSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SubDocPatchIteratorTest, TestSimpleProcess);
}} // namespace indexlib::index
