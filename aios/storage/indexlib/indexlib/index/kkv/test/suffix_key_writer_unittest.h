#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index/kkv/suffix_key_writer.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class SuffixKeyWriterTest : public INDEXLIB_TESTBASE
{
public:
    SuffixKeyWriterTest();
    ~SuffixKeyWriterTest();

    DECLARE_CLASS_NAME(SuffixKeyDumperTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestLinkSkeyNode();

private:
    typedef SuffixKeyWriter<uint32_t> SKeyWriter;
    typedef std::vector<SKeyWriter::SKeyNode> SKeyNodeVector;

private:
    void MakeData(SKeyNodeVector& skeyNodes, uint32_t count);
    void MakeResult(const SKeyNodeVector& skeyNodes, SKeyNodeVector& result);
    void CheckResult(const SKeyWriter& skeyWriter, const SKeyListInfo& listInfo, const SKeyNodeVector& result);

    void InnerTestLinkSkeyNode(uint32_t count, uint32_t threshold);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SuffixKeyWriterTest, TestLinkSkeyNode);
}} // namespace indexlib::index
