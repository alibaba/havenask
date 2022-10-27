#ifndef __INDEXLIB_LAZYLOADSINGLEVALUEATTRIBUTEREADERTEST_H
#define __INDEXLIB_LAZYLOADSINGLEVALUEATTRIBUTEREADERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/lazy_load_single_value_attribute_reader.h"

DECLARE_REFERENCE_CLASS(index, AttributeReader);
DECLARE_REFERENCE_CLASS(file_system, Directory);
IE_NAMESPACE_BEGIN(index);

class LazyLoadSingleValueAttributeReaderTest : public INDEXLIB_TESTBASE
{
public:
    LazyLoadSingleValueAttributeReaderTest();
    ~LazyLoadSingleValueAttributeReaderTest();

    DECLARE_CLASS_NAME(LazyLoadSingleValueAttributeReaderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestReadWithPatchFiles();
private:
    void CheckValues(const file_system::DirectoryPtr& rootDir,
                     const AttributeReaderPtr& attrReader,
                     const std::string& expectValueString);
private:
    std::string mRootDir;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(LazyLoadSingleValueAttributeReaderTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(LazyLoadSingleValueAttributeReaderTest, TestReadWithPatchFiles);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_LAZYLOADSINGLEVALUEATTRIBUTEREADERTEST_H
