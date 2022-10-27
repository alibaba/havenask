#ifndef __INDEXLIB_VARNUMATTRIBUTEREADERREADPERFTEST_H
#define __INDEXLIB_VARNUMATTRIBUTEREADERREADPERFTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_reader.h"

IE_NAMESPACE_BEGIN(index);

class VarNumAttributeReaderReadPerfTest : public INDEXLIB_TESTBASE
{
public:
    VarNumAttributeReaderReadPerfTest();
    ~VarNumAttributeReaderReadPerfTest();

    DECLARE_CLASS_NAME(VarNumAttributeReaderReadPerfTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestRead();

private:
    void InnerRead(const VarNumAttributeReader<int16_t>& attrReader, 
                   docid_t startDocId, docid_t endDocId);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(VarNumAttributeReaderReadPerfTest, TestRead);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_VARNUMATTRIBUTEREADERREADPERFTEST_H
