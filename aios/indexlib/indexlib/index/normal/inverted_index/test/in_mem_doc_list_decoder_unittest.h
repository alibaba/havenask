#ifndef __INDEXLIB_INMEMDOCLISTDECODERTEST_H
#define __INDEXLIB_INMEMDOCLISTDECODERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/format/in_mem_doc_list_decoder.h"
#include "indexlib/index/normal/inverted_index/format/doc_list_encoder.h"
#include <autil/mem_pool/SimpleAllocator.h>

IE_NAMESPACE_BEGIN(index);

class InMemDocListDecoderTest : public INDEXLIB_TESTBASE {
public:
    InMemDocListDecoderTest();
    ~InMemDocListDecoderTest();

    DECLARE_CLASS_NAME(InMemDocListDecoderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestDecodeDocBufferWithoutSkipList();
    void TestDecodeDocBufferWithoutSkipListAndWithDocByteSlice();
    void TestDecodeDocBufferWithSkipList();
    void TestDecodeDocBufferWithOneMoreDocByteSlice();
    void TestDecodeDocBufferWithTwoMoreDocByteSlice();
    void TestDecodeCurrentTFBuffer();
    void TestDecodeCurrentDocPayloadBuffer();
    void TestDecodeCurrentFieldMapBuffer();
    void TestDecodeAllBuffer();
    void TestDecode();
    void TestDecodeWithFlush();
    void TestDecodeHasTf();
    void TestDecodeHasTfWithFlush();

private:
    void TestDecodeWithOptionFlag(const optionflag_t flag, size_t docNum,
                                  docid_t* docids, tf_t* tfs,
                                  docpayload_t* docPayloads, 
                                  fieldmap_t* fieldMaps);

    InMemDocListDecoder* CreateDecoder(uint32_t docCount, bool needFlush,
            bool needTf);
    void TestDecode(const uint32_t docCount, bool needFlush = false, 
                    bool needTf = false);

private:
    autil::mem_pool::RecyclePool* mBufferPool;
    autil::mem_pool::Pool* mByteSlicePool;
    util::SimplePool mSimplePool;
    autil::mem_pool::SimpleAllocator mAllocator;
    InMemDocListDecoder* mDocListDecoder;
    DocListEncoder* mDocListEncoder;

    DocListFormatPtr mDocListFormatPtr;
    DocListEncoderPtr mDocListEncoderPtr;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InMemDocListDecoderTest, TestDecodeDocBufferWithoutSkipList);
INDEXLIB_UNIT_TEST_CASE(InMemDocListDecoderTest, TestDecodeDocBufferWithoutSkipListAndWithDocByteSlice);
INDEXLIB_UNIT_TEST_CASE(InMemDocListDecoderTest, TestDecodeDocBufferWithSkipList);
INDEXLIB_UNIT_TEST_CASE(InMemDocListDecoderTest, TestDecodeDocBufferWithOneMoreDocByteSlice);
INDEXLIB_UNIT_TEST_CASE(InMemDocListDecoderTest, TestDecodeDocBufferWithTwoMoreDocByteSlice);
INDEXLIB_UNIT_TEST_CASE(InMemDocListDecoderTest, TestDecodeCurrentTFBuffer);
INDEXLIB_UNIT_TEST_CASE(InMemDocListDecoderTest, TestDecodeCurrentDocPayloadBuffer);
INDEXLIB_UNIT_TEST_CASE(InMemDocListDecoderTest, TestDecodeCurrentFieldMapBuffer);
INDEXLIB_UNIT_TEST_CASE(InMemDocListDecoderTest, TestDecodeAllBuffer);
INDEXLIB_UNIT_TEST_CASE(InMemDocListDecoderTest, TestDecode);
INDEXLIB_UNIT_TEST_CASE(InMemDocListDecoderTest, TestDecodeWithFlush);
INDEXLIB_UNIT_TEST_CASE(InMemDocListDecoderTest, TestDecodeHasTf);
INDEXLIB_UNIT_TEST_CASE(InMemDocListDecoderTest, TestDecodeHasTfWithFlush);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INMEMDOCLISTDECODERTEST_H
