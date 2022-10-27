#include <autil/mem_pool/Pool.h>
#include <autil/mem_pool/RecyclePool.h>
#include <autil/StringUtil.h>
#include "indexlib/index/normal/inverted_index/format/posting_format_option.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_writer_impl.h"
#include "indexlib/index/normal/inverted_index/test/position_list_segment_decoder_unittest.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_in_doc_state.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/in_mem_file_node_creator.h"
#include "indexlib/file_system/buffered_file_writer.h"

using namespace std;
using namespace autil::mem_pool;
using namespace autil;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PositionListSegmentDecoderTest);

PositionListSegmentDecoderTest::PositionListSegmentDecoderTest()
    : mByteSlicePool(10240)
    , mBufferPool(10240)
{
}

PositionListSegmentDecoderTest::~PositionListSegmentDecoderTest()
{
}

void PositionListSegmentDecoderTest::CaseSetUp()
{
    mDir = GET_TEST_DATA_PATH();
}

void PositionListSegmentDecoderTest::CaseTearDown()
{
}

void PositionListSegmentDecoderTest::TestSkipAndLocateAndDecodeWithTFBitmap()
{
    optionflag_t flag = of_position_list | of_tf_bitmap | of_term_frequency;

    InnerTestSkipAndLocateAndDecode(flag, "1,2");             // ttf < MAX_UNCOMPRESSED_POS_LIST_SIZE
    InnerTestSkipAndLocateAndDecode(flag, "1,2,2");           // ttf = MAX_UNCOMPRESSED_POS_LIST_SIZE
    InnerTestSkipAndLocateAndDecode(flag, "5,1");             // ttf > MAX_UNCOMPRESSED_POS_LIST_SIZE
    InnerTestSkipAndLocateAndDecode(flag, "28,50,50");        // ttf = MAX_POS_PER_RECORD
    InnerTestSkipAndLocateAndDecode(flag, "100,50");          // ttf > MAX_POS_PER_RECORD, doc cross two block
    InnerTestSkipAndLocateAndDecode(flag, "118,187,50");      // ttf > MAX_POS_PER_RECORD, doc cross three block
    InnerTestSkipAndLocateAndDecode(flag, "100,287,350");     // ttf > MAX_POS_PER_RECORD, doc cross many block
    InnerTestSkipAndLocateAndDecode(flag, "500,280,500");     // ttf = MAX_POS_PER_RECORD * MAX_UNCOMPRESSED_SKIP_LIST_SIZE
    InnerTestSkipAndLocateAndDecode(flag, "500,280,600");     // ttf > MAX_POS_PER_RECORD * MAX_UNCOMPRESSED_SKIP_LIST_SIZE
    InnerTestSkipAndLocateAndDecode(flag, "2048,1024,1024");  // ttf = MAX_POS_PER_RECORD * SKIP_LIST_BUFFER_SIZE
    InnerTestSkipAndLocateAndDecode(flag, "1,4096,2");        // ttf > MAX_POS_PER_RECORD * SKIP_LIST_BUFFER_SIZE
    InnerTestSkipAndLocateAndDecode(flag, "1,4096,4096,2");
}

void PositionListSegmentDecoderTest::TestSkipAndLocateAndDecode()
{
    optionflag_t flag = of_position_list | of_term_frequency;

    InnerTestSkipAndLocateAndDecode(flag, "1,2");             // ttf < MAX_UNCOMPRESSED_POS_LIST_SIZE
    InnerTestSkipAndLocateAndDecode(flag, "1,2,2");           // ttf = MAX_UNCOMPRESSED_POS_LIST_SIZE
    InnerTestSkipAndLocateAndDecode(flag, "5,1");             // ttf > MAX_UNCOMPRESSED_POS_LIST_SIZE
    InnerTestSkipAndLocateAndDecode(flag, "28,50,50");        // ttf = MAX_POS_PER_RECORD
    InnerTestSkipAndLocateAndDecode(flag, "100,50");          // ttf > MAX_POS_PER_RECORD, doc cross two block
    InnerTestSkipAndLocateAndDecode(flag, "118,187,50");      // ttf > MAX_POS_PER_RECORD, doc cross three block
    InnerTestSkipAndLocateAndDecode(flag, "100,287,350");     // ttf > MAX_POS_PER_RECORD, doc cross many block
    InnerTestSkipAndLocateAndDecode(flag, "500,280,500");     // ttf = MAX_POS_PER_RECORD * MAX_UNCOMPRESSED_SKIP_LIST_SIZE
    InnerTestSkipAndLocateAndDecode(flag, "500,280,600");     // ttf > MAX_POS_PER_RECORD * MAX_UNCOMPRESSED_SKIP_LIST_SIZE
    InnerTestSkipAndLocateAndDecode(flag, "2048,1024,1024");  // ttf = MAX_POS_PER_RECORD * SKIP_LIST_BUFFER_SIZE
    InnerTestSkipAndLocateAndDecode(flag, "1,4096,2");        // ttf > MAX_POS_PER_RECORD * SKIP_LIST_BUFFER_SIZE
    InnerTestSkipAndLocateAndDecode(flag, "1,4096,4096,2");
}

void PositionListSegmentDecoderTest::InnerTestSkipAndLocateAndDecode(
        optionflag_t optionFlag, const string& docStrs)
{
    // prepare position list

    int32_t posListBegin = 0;
    int32_t posDataListBegin = 0;
    ByteSliceListPtr posList(PreparePositionList(optionFlag, docStrs,
                    posListBegin, posDataListBegin));

    PositionListSegmentDecoder decoder(optionFlag, &mByteSlicePool);

    ttf_t totalTf = 0;
    vector<tf_t> tfList;
    StringUtil::fromString(docStrs, tfList, ",");
    for (size_t i = 0; i < tfList.size(); ++i)
    {
        totalTf += tfList[i];
    }

    uint32_t termFrequence;
    uint8_t compressMode = ShortListOptimizeUtil::GetPosListCompressMode(totalTf);

    NormalInDocState state(optionFlag, compressMode);
    decoder.Init(posList.get(), totalTf, posListBegin, 
                 compressMode, &state);

    // TODO: change with optionFlag
    // TODO: remove specilization assert
    // ASSERT_TRUE(decoder.mPosSkipListReader != NULL);
    // ASSERT_TRUE(decoder.mBitmapReader == NULL);
    // ASSERT_TRUE(decoder.mPosBitmapBlockBuffer == NULL);

    ttf_t preAggTotalTf = 0;
    ttf_t decodePosCount = 0;
    uint32_t expectRecordOffset = 0;
    for (size_t i = 0; i < tfList.size(); ++i)
    {
        state.SetSeekedDocCount(i + 1);
        ASSERT_TRUE(decoder.SkipTo(preAggTotalTf, &state));

        pos_t lastPos = 0;
        if (preAggTotalTf >= decodePosCount)
        {
            ASSERT_TRUE(decoder.LocateRecord(&state, termFrequence));
            expectRecordOffset = decoder.mPosListReader.Tell();
        }
        else
        {
            ASSERT_FALSE(decoder.LocateRecord(&state, termFrequence));
        }

        ASSERT_EQ(expectRecordOffset, decoder.GetRecordOffset());
        ttf_t prevAlignedDecodePosCount = (preAggTotalTf) 
                                      / MAX_POS_PER_RECORD * MAX_POS_PER_RECORD;

        int32_t expectedOffset = preAggTotalTf - prevAlignedDecodePosCount;
        ASSERT_EQ(expectedOffset, decoder.GetOffsetInRecord());

        for (tf_t j = 0; j < tfList[i]; ++j)
        {
            ttf_t curPosIdx = preAggTotalTf + j;
            if (curPosIdx >= decodePosCount)
            {
                expectRecordOffset = decoder.mPosListReader.Tell();
                uint32_t decodeCount = decoder.DecodeRecord(mPosBuffer, MAX_POS_PER_RECORD,
                        mPosPayloadBuffer, MAX_POS_PER_RECORD);
                decodePosCount += decodeCount;
            }

            pos_t curPos = lastPos + mPosBuffer[curPosIdx % MAX_POS_PER_RECORD];
            lastPos = curPos;
            ASSERT_EQ((pos_t)j, curPos);
        }
        preAggTotalTf += tfList[i];
    }
    ASSERT_EQ(totalTf, decodePosCount);
    if (!(optionFlag & of_tf_bitmap))
    {
        ASSERT_FALSE(decoder.SkipTo(preAggTotalTf, &state));
    }
}

ByteSliceList* PositionListSegmentDecoderTest::PreparePositionList(
        optionflag_t optionFlag, const std::string& docStrs,
        int32_t& posListBegin, int32_t& posDataListBegin)
{
    vector<tf_t> tfList;
    StringUtil::fromString(docStrs, tfList, ",");

    PostingFormatOption postingFormatOption(optionFlag);
    PostingWriterResource writerResource(&mSimplePool, &mByteSlicePool,
            &mBufferPool, postingFormatOption);
    PostingWriterImpl postingWriter(&writerResource);

    ttf_t totalTF = 0;
    for (size_t docIdx = 0; docIdx < tfList.size(); ++docIdx)
    {
        for (tf_t posIdx = 0; posIdx < tfList[docIdx]; ++posIdx)
        {
            postingWriter.AddPosition(posIdx, 0, 0);
        }
        postingWriter.EndDocument((docid_t)docIdx, 0);
        totalTF += tfList[docIdx];
    }
    postingWriter.EndSegment();

    file_system::BufferedFileWriterPtr fileWriter(new file_system::BufferedFileWriter);
    string posListFile = mDir + "posting_file";
    FileSystemWrapper::DeleteIfExist(posListFile);
    fileWriter->Open(posListFile);
    postingWriter.Dump(fileWriter);
    fileWriter->Close();

    size_t fileLength = FileSystemWrapper::GetFileLength(posListFile);
    DocListEncoder* docListEncoder = postingWriter.GetDocListEncoder();
    assert(docListEncoder);
    posListBegin = docListEncoder->GetDumpLength();
    index::PositionBitmapWriter* tfBitmapWriter = 
        docListEncoder->GetPositionBitmapWriter();
    if (tfBitmapWriter)
    {
        posListBegin -= tfBitmapWriter->GetDumpLength(totalTF);
    }

    const ByteSliceList* posByteSliceList = postingWriter.GetPositionList();
    size_t positionListLen = (posByteSliceList == NULL)? 0 :
                             posByteSliceList->GetTotalSize();
    posDataListBegin = fileLength - positionListLen;

    mFileReader.reset(file_system::InMemFileNodeCreator::Create());
    mFileReader->Open(posListFile, file_system::FSOT_IN_MEM);
    mFileReader->Populate();
    return mFileReader->Read(fileLength, 0);
}

IE_NAMESPACE_END(index);

