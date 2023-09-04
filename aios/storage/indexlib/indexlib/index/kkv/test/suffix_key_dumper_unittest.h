#ifndef __INDEXLIB_SUFFIXKEYDUMPERTEST_H
#define __INDEXLIB_SUFFIXKEYDUMPERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/index/kkv/suffix_key_dumper.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class SuffixKeyDumperTest : public INDEXLIB_TESTBASE
{
public:
    SuffixKeyDumperTest();
    ~SuffixKeyDumperTest();

    DECLARE_CLASS_NAME(SuffixKeyDumperTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    // skeyNodeInfoStr: pkey1:skeyCount;pkey2;skeyCount#pkey;skey[other chunk]
    // resultStr: pkey,chunkOffset,inChunkOffset;...
    void InnerTestSimpleProcess(const std::string& skeyNodeInfoStr, const std::string& resultStrBeforeClose,
                                const std::string& resultStrAfterClose, size_t dataLen);

    void CheckIter(const DumpablePKeyOffsetIteratorPtr& pkeyIter, const std::string& resultStr, regionid_t regionId);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SuffixKeyDumperTest, TestSimpleProcess);
}} // namespace indexlib::index

#endif //__INDEXLIB_SUFFIXKEYDUMPERTEST_H
