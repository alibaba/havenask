#ifndef __INDEXLIB_LAZYLOADVARNUMATTRIBUTEREADERTEST_H
#define __INDEXLIB_LAZYLOADVARNUMATTRIBUTEREADERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/lazy_load_var_num_attribute_reader.h"

DECLARE_REFERENCE_CLASS(index, AttributeReader);
DECLARE_REFERENCE_CLASS(file_system, Directory);
IE_NAMESPACE_BEGIN(index);

class LazyLoadVarNumAttributeReaderTest : public INDEXLIB_TESTBASE
{
public:
    LazyLoadVarNumAttributeReaderTest();
    ~LazyLoadVarNumAttributeReaderTest();

    DECLARE_CLASS_NAME(LazyLoadVarNumAttributeReaderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestReadWithInMemSegment(); 
    void TestReadWithPatchFiles();
    void TestReadStringAttribute();
    void TestReadMultiStringAttribute(); 
    void TestReadLocationAttribute(); 
private:
    void CheckValues(const file_system::DirectoryPtr& rootDir,
                     const AttributeReaderPtr& attrReader,
                     const std::string& expectValueString,
                     bool isAttrUpdatable,
                     bool checkLoadMode = true);
private:
    std::string mRootDir;    
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(LazyLoadVarNumAttributeReaderTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(LazyLoadVarNumAttributeReaderTest, TestReadWithInMemSegment);
INDEXLIB_UNIT_TEST_CASE(LazyLoadVarNumAttributeReaderTest, TestReadWithPatchFiles);
INDEXLIB_UNIT_TEST_CASE(LazyLoadVarNumAttributeReaderTest, TestReadStringAttribute);
INDEXLIB_UNIT_TEST_CASE(LazyLoadVarNumAttributeReaderTest, TestReadMultiStringAttribute);
INDEXLIB_UNIT_TEST_CASE(LazyLoadVarNumAttributeReaderTest, TestReadLocationAttribute);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_LAZYLOADVARNUMATTRIBUTEREADERTEST_H
