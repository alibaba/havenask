#include "indexlib/index/inverted_index/format/PositionListSegmentDecoder.h"

#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/RecyclePool.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/file/MemFileNode.h"
#include "indexlib/file_system/file/MemFileNodeCreator.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/inverted_index/PostingWriterImpl.h"
#include "indexlib/index/inverted_index/format/BufferedByteSliceReader.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/index/inverted_index/format/skiplist/BufferedSkipListWriter.h"
#include "indexlib/util/SimplePool.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class PositionListSegmentDecoderTest : public TESTBASE
{
public:
    PositionListSegmentDecoderTest() : _byteSlicePool(10240), _bufferPool(10240) {}
    ~PositionListSegmentDecoderTest() {}

    void setUp() override { _dir = GET_TEMP_DATA_PATH(); }
    void tearDown() override {}

private:
    void InnerTestSkipAndLocateAndDecode(optionflag_t optionFlag, const std::string& docStrs)
    {
        // prepare position list

        int32_t posListBegin = 0;
        int32_t posDataListBegin = 0;
        util::ByteSliceList* byteSliceList = nullptr;
        PreparePositionList(optionFlag, docStrs, posListBegin, posDataListBegin, byteSliceList);
        util::ByteSliceListPtr posList(byteSliceList);

        PositionListSegmentDecoder decoder(optionFlag, &_byteSlicePool);

        ttf_t totalTf = 0;
        std::vector<tf_t> tfList;
        autil::StringUtil::fromString(docStrs, tfList, ",");
        for (size_t i = 0; i < tfList.size(); ++i) {
            totalTf += tfList[i];
        }

        uint32_t termFrequence;
        uint8_t compressMode = ShortListOptimizeUtil::GetPosListCompressMode(totalTf);

        NormalInDocState state(optionFlag, compressMode);
        decoder.Init(posList.get(), totalTf, posListBegin, compressMode, &state);

        // TODO: change with optionFlag
        // TODO: remove specilization assert
        // ASSERT_TRUE(decoder.mPosSkipListReader != nullptr);
        // ASSERT_TRUE(decoder.mBitmapReader == nullptr);
        // ASSERT_TRUE(decoder.mPosBitmapBlockBuffer == nullptr);

        ttf_t preAggTotalTf = 0;
        ttf_t decodePosCount = 0;
        uint32_t expectRecordOffset = 0;
        for (size_t i = 0; i < tfList.size(); ++i) {
            state.SetSeekedDocCount(i + 1);
            ASSERT_TRUE(decoder.SkipTo(preAggTotalTf, &state));

            pos_t lastPos = 0;
            if (preAggTotalTf >= decodePosCount) {
                ASSERT_TRUE(decoder.LocateRecord(&state, termFrequence));
                expectRecordOffset = decoder._posListReader.Tell();
            } else {
                ASSERT_FALSE(decoder.LocateRecord(&state, termFrequence));
            }

            ASSERT_EQ(expectRecordOffset, decoder.GetRecordOffset());
            ttf_t prevAlignedDecodePosCount = (preAggTotalTf) / MAX_POS_PER_RECORD * MAX_POS_PER_RECORD;

            int32_t expectedOffset = preAggTotalTf - prevAlignedDecodePosCount;
            ASSERT_EQ(expectedOffset, decoder.GetOffsetInRecord());

            for (tf_t j = 0; j < tfList[i]; ++j) {
                ttf_t curPosIdx = preAggTotalTf + j;
                if (curPosIdx >= decodePosCount) {
                    expectRecordOffset = decoder._posListReader.Tell();
                    uint32_t decodeCount =
                        decoder.DecodeRecord(_posBuffer, MAX_POS_PER_RECORD, _posPayloadBuffer, MAX_POS_PER_RECORD);
                    decodePosCount += decodeCount;
                }

                pos_t curPos = lastPos + _posBuffer[curPosIdx % MAX_POS_PER_RECORD];
                lastPos = curPos;
                ASSERT_EQ((pos_t)j, curPos);
            }
            preAggTotalTf += tfList[i];
        }
        ASSERT_EQ(totalTf, decodePosCount);
        if (!(optionFlag & of_tf_bitmap)) {
            ASSERT_FALSE(decoder.SkipTo(preAggTotalTf, &state));
        }
    }

    // docStrs : tf1,tf2
    void PreparePositionList(optionflag_t optionFlag, const std::string& docStrs, int32_t& posListBegin,
                             int32_t& posDataListBegin, util::ByteSliceList*& sliceList)
    {
        std::vector<tf_t> tfList;
        autil::StringUtil::fromString(docStrs, tfList, ",");

        PostingFormatOption postingFormatOption(optionFlag);
        PostingWriterResource writerResource(&_simplePool, &_byteSlicePool, &_bufferPool, postingFormatOption);
        PostingWriterImpl postingWriter(&writerResource);

        ttf_t totalTF = 0;
        for (size_t docIdx = 0; docIdx < tfList.size(); ++docIdx) {
            for (tf_t posIdx = 0; posIdx < tfList[docIdx]; ++posIdx) {
                postingWriter.AddPosition(posIdx, 0, 0);
            }
            postingWriter.EndDocument((docid_t)docIdx, 0);
            totalTF += tfList[docIdx];
        }
        postingWriter.EndSegment();

        file_system::BufferedFileWriterPtr fileWriter(new file_system::BufferedFileWriter);
        std::string posListFile = _dir + "posting_file";
        file_system::FslibWrapper::DeleteFileE(posListFile, file_system::DeleteOption::NoFence(true));
        EXPECT_EQ(file_system::FSEC_OK, fileWriter->Open(posListFile, posListFile));
        postingWriter.Dump(fileWriter);
        EXPECT_EQ(file_system::FSEC_OK, fileWriter->Close());

        size_t fileLength = file_system::FslibWrapper::GetFileLength(posListFile).GetOrThrow();
        DocListEncoder* docListEncoder = postingWriter.GetDocListEncoder();
        assert(docListEncoder);
        posListBegin = docListEncoder->GetDumpLength();
        index::PositionBitmapWriter* tfBitmapWriter = docListEncoder->GetPositionBitmapWriter();
        if (tfBitmapWriter) {
            posListBegin -= tfBitmapWriter->GetDumpLength(totalTF);
        }

        const util::ByteSliceList* posByteSliceList = postingWriter.GetPositionList();
        size_t positionListLen = (posByteSliceList == nullptr) ? 0 : posByteSliceList->GetTotalSize();
        posDataListBegin = fileLength - positionListLen;

        _fileReader.reset(file_system::MemFileNodeCreator::TEST_Create());
        EXPECT_EQ(file_system::FSEC_OK, _fileReader->Open("", posListFile, file_system::FSOT_MEM, -1));
        EXPECT_EQ(file_system::FSEC_OK, _fileReader->Populate());
        sliceList = _fileReader->ReadToByteSliceList(fileLength, 0, file_system::ReadOption());
        EXPECT_TRUE(sliceList);
    }

    std::string _dir;
    autil::mem_pool::Pool _byteSlicePool;
    autil::mem_pool::RecyclePool _bufferPool;
    util::SimplePool _simplePool;
    std::shared_ptr<file_system::MemFileNode> _fileReader;

    pos_t _posBuffer[MAX_POS_PER_RECORD];
    pospayload_t _posPayloadBuffer[MAX_POS_PER_RECORD];
};

TEST_F(PositionListSegmentDecoderTest, testSkipAndLocateAndDecodeWithTFBitmap)
{
    optionflag_t flag = of_position_list | of_tf_bitmap | of_term_frequency;

    InnerTestSkipAndLocateAndDecode(flag, "1,2");         // ttf < MAX_UNCOMPRESSED_POS_LIST_SIZE
    InnerTestSkipAndLocateAndDecode(flag, "1,2,2");       // ttf = MAX_UNCOMPRESSED_POS_LIST_SIZE
    InnerTestSkipAndLocateAndDecode(flag, "5,1");         // ttf > MAX_UNCOMPRESSED_POS_LIST_SIZE
    InnerTestSkipAndLocateAndDecode(flag, "28,50,50");    // ttf = MAX_POS_PER_RECORD
    InnerTestSkipAndLocateAndDecode(flag, "100,50");      // ttf > MAX_POS_PER_RECORD, doc cross two block
    InnerTestSkipAndLocateAndDecode(flag, "118,187,50");  // ttf > MAX_POS_PER_RECORD, doc cross three block
    InnerTestSkipAndLocateAndDecode(flag, "100,287,350"); // ttf > MAX_POS_PER_RECORD, doc cross many block
    InnerTestSkipAndLocateAndDecode(flag, "500,280,500"); // ttf = MAX_POS_PER_RECORD * MAX_UNCOMPRESSED_SKIP_LIST_SIZE
    InnerTestSkipAndLocateAndDecode(flag, "500,280,600"); // ttf > MAX_POS_PER_RECORD * MAX_UNCOMPRESSED_SKIP_LIST_SIZE
    InnerTestSkipAndLocateAndDecode(flag, "2048,1024,1024"); // ttf = MAX_POS_PER_RECORD * SKIP_LIST_BUFFER_SIZE
    InnerTestSkipAndLocateAndDecode(flag, "1,4096,2");       // ttf > MAX_POS_PER_RECORD * SKIP_LIST_BUFFER_SIZE
    InnerTestSkipAndLocateAndDecode(flag, "1,4096,4096,2");
}

TEST_F(PositionListSegmentDecoderTest, testSkipAndLocateAndDecode)
{
    optionflag_t flag = of_position_list | of_term_frequency;

    InnerTestSkipAndLocateAndDecode(flag, "1,2");         // ttf < MAX_UNCOMPRESSED_POS_LIST_SIZE
    InnerTestSkipAndLocateAndDecode(flag, "1,2,2");       // ttf = MAX_UNCOMPRESSED_POS_LIST_SIZE
    InnerTestSkipAndLocateAndDecode(flag, "5,1");         // ttf > MAX_UNCOMPRESSED_POS_LIST_SIZE
    InnerTestSkipAndLocateAndDecode(flag, "28,50,50");    // ttf = MAX_POS_PER_RECORD
    InnerTestSkipAndLocateAndDecode(flag, "100,50");      // ttf > MAX_POS_PER_RECORD, doc cross two block
    InnerTestSkipAndLocateAndDecode(flag, "118,187,50");  // ttf > MAX_POS_PER_RECORD, doc cross three block
    InnerTestSkipAndLocateAndDecode(flag, "100,287,350"); // ttf > MAX_POS_PER_RECORD, doc cross many block
    InnerTestSkipAndLocateAndDecode(flag, "500,280,500"); // ttf = MAX_POS_PER_RECORD * MAX_UNCOMPRESSED_SKIP_LIST_SIZE
    InnerTestSkipAndLocateAndDecode(flag, "500,280,600"); // ttf > MAX_POS_PER_RECORD * MAX_UNCOMPRESSED_SKIP_LIST_SIZE
    InnerTestSkipAndLocateAndDecode(flag, "2048,1024,1024"); // ttf = MAX_POS_PER_RECORD * SKIP_LIST_BUFFER_SIZE
    InnerTestSkipAndLocateAndDecode(flag, "1,4096,2");       // ttf > MAX_POS_PER_RECORD * SKIP_LIST_BUFFER_SIZE
    InnerTestSkipAndLocateAndDecode(flag, "1,4096,4096,2");
}

} // namespace indexlib::index
