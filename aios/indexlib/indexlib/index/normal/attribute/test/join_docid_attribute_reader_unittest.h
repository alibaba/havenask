#ifndef __INDEXLIB_JOINDOCIDATTRIBUTEREADERTEST_H
#define __INDEXLIB_JOINDOCIDATTRIBUTEREADERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/join_docid_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/join_docid_attribute_iterator.h"
#include "indexlib/test/single_field_partition_data_provider.h"

IE_NAMESPACE_BEGIN(index);

class JoinDocidAttributeReaderTest : public INDEXLIB_TESTBASE_WITH_PARAM<bool>
{
public:
    JoinDocidAttributeReaderTest();
    ~JoinDocidAttributeReaderTest();

    DECLARE_CLASS_NAME(JoinDocidAttributeReaderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestReadOverflow();

private:
    JoinDocidAttributeReaderPtr CreateReader(std::string mainInc,
            std::string mainRt, std::string subInc, std::string subRt);
    docid_t GetDocId(docid_t docId) const;

private:
    std::string mMainRoot;
    std::string mSubRoot;
    test::SingleFieldPartitionDataProviderPtr mMainProvider;
    test::SingleFieldPartitionDataProviderPtr mSubProvider;
    JoinDocidAttributeReaderPtr mReader;
    JoinDocidAttributeIterator* mIter;
    autil::mem_pool::Pool *mPool;
    bool mUseIter;

private:
    IE_LOG_DECLARE();
};

INSTANTIATE_TEST_CASE_P(p1, JoinDocidAttributeReaderTest, testing::Values(true, false));
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(JoinDocidAttributeReaderTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(JoinDocidAttributeReaderTest, TestReadOverflow);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_JOINDOCIDATTRIBUTEREADERTEST_H
