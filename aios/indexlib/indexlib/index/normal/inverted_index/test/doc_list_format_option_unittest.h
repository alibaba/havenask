#ifndef __INDEXLIB_DOCLISTFORMATOPTIONTEST_H
#define __INDEXLIB_DOCLISTFORMATOPTIONTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/format/doc_list_format_option.h"

IE_NAMESPACE_BEGIN(index);

class DocListFormatOptionTest : public INDEXLIB_TESTBASE {
public:
    DocListFormatOptionTest();
    ~DocListFormatOptionTest();
public:
    void SetUp();
    void TearDown();
    void TestSimpleProcess();
    void TestInit();
    void TestCopyConstructor();
    void TestJsonize();

private:
    void InnerTestInitDocListMeta(optionflag_t optionFlag,
        bool expectHasTf, bool expectHasTfList, bool expectHasTfBitmap, 
        bool expectHasDocPayload, bool expectHasFieldMap);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DocListFormatOptionTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(DocListFormatOptionTest, TestInit);
INDEXLIB_UNIT_TEST_CASE(DocListFormatOptionTest, TestCopyConstructor);
INDEXLIB_UNIT_TEST_CASE(DocListFormatOptionTest, TestJsonize);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DOCLISTFORMATOPTIONTEST_H
