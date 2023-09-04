#include "indexlib/index/normal/inverted_index/test/normal_in_doc_position_iterator_unittest.h"

#include "indexlib/file_system/file/MemFileNode.h"
#include "indexlib/file_system/file/MemFileNodeCreator.h"
#include "indexlib/index/inverted_index/format/PositionListEncoder.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace autil::mem_pool;

using namespace indexlib::util;
using namespace indexlib::common;
using namespace indexlib::file_system;
using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::document;

namespace indexlib { namespace index {

void NormalInDocPositionIteratorTest::CaseSetUp()
{
    mDir = GET_TEMP_DATA_PATH();
    mSchema = AttributeTestUtil::MakeSchemaWithPackageIndexConfig("test_pack", MAX_FIELD_COUNT);

    mIndexConfig = DYNAMIC_POINTER_CAST(PackageIndexConfig, mSchema->GetIndexSchema()->GetIndexConfig("test_pack"));
}

void NormalInDocPositionIteratorTest::CaseTearDown() {}

void NormalInDocPositionIteratorTest::TestCaseForSectionFeature() { TestForSectionFeature(OPTION_FLAG_ALL); }

void NormalInDocPositionIteratorTest::TestCaseForNoPayloadSectionFeature() { TestForSectionFeature(NO_PAYLOAD); }

void NormalInDocPositionIteratorTest::TestCaseForNoPositionListSectionFeature()
{
    TestForSectionFeature(NO_POSITION_LIST);
}

void NormalInDocPositionIteratorTest::TestCaseForTfBitmap() { TestForSectionFeature(OPTION_FLAG_ALL | of_tf_bitmap); }

void NormalInDocPositionIteratorTest::TestForSectionFeature(optionflag_t optionFlag)
{
    tearDown();
    setUp();
    // "docid docPayload, (pos posPayload, pos posPayload, ...); ....
    string str = "1 3, (1 3, 4 6, 8 1, 12 1, 17 8); "
                 "4 1, (4 2, 17 1)";

    // "fieldid len weight, ...; ..."
    string sectionStr = "0 1 1;"
                        "0 3 7, 1 9 4, 2 5 3, 2 6 7;" // 3 12 17 23
                        "0 1 1;"
                        "0 1 1;"
                        "0 2 1, 1 2 2, 1 2 2, 2 5 4, 2 9 8;"; // 2 4 6 11 20
    string filePath = mDir + "bufferFile";
    AnswerMap answerMap;

    mIndexConfig->SetOptionFlag(optionFlag);
    IndexFormatOption indexFormatOption;
    indexFormatOption.Init(mIndexConfig);

    IndexTestUtil::BuildOneSegmentFromDataString(str, filePath, 0, answerMap, indexFormatOption);

    file_system::MemFileNodePtr fileReader(file_system::MemFileNodeCreator::TEST_Create());
    ASSERT_EQ(file_system::FSEC_OK, fileReader->Open("", filePath, file_system::FSOT_MEM, -1));
    ASSERT_EQ(file_system::FSEC_OK, fileReader->Populate());
    FileMeta meta;
    FileSystem::getFileMeta(filePath, meta);
    ByteSliceListPtr sliceListPtr(fileReader->ReadToByteSliceList(meta.fileLength, 0, ReadOption()));
    assert(sliceListPtr);

    LegacySectionAttributeReaderImplPtr sectionReaderImpl;
    if (optionFlag & of_position_list) {
        SectionDataMaker::BuildOneSegmentSectionData(mSchema, mIndexConfig->GetIndexName(), sectionStr,
                                                     GET_PARTITION_DIRECTORY(), 0);
        sectionReaderImpl = PrepareSectionReader();
    }

    PostingFormatOption postingFormatOption(optionFlag);
    BufferedPostingIterator iter(postingFormatOption, NULL, nullptr);

    uint8_t compressMode = ShortListOptimizeUtil::GetCompressMode((df_t)2, (ttf_t)7, PFOR_DELTA_COMPRESS_MODE);
    SegmentPosting segPosting(postingFormatOption);
    segPosting.Init(compressMode, sliceListPtr, 0, 0);
    shared_ptr<SegmentPostingVector> segPostings(new SegmentPostingVector);
    segPostings->push_back(segPosting);
    iter.Init(segPostings, sectionReaderImpl.get(), 5);

    INDEXLIB_TEST_EQUAL(1, iter.SeekDoc(1));

    TermMatchData termMathData;
    iter.Unpack(termMathData);
    if (optionFlag & of_doc_payload) {
        INDEXLIB_TEST_EQUAL(3, termMathData.GetDocPayload());
    }

    std::shared_ptr<InDocPositionIterator> posIter = termMathData.GetInDocPositionIterator();

    if (optionFlag & of_position_list) {
        INDEXLIB_TEST_TRUE(posIter != NULL);
        INDEXLIB_TEST_EQUAL((pos_t)1, posIter->SeekPosition(0));
        if (optionFlag & of_position_payload) {
            INDEXLIB_TEST_EQUAL((pospayload_t)3, posIter->GetPosPayload());
        } else {
            INDEXLIB_TEST_EQUAL((pospayload_t)INVALID_POS_PAYLOAD, posIter->GetPosPayload());
        }
        INDEXLIB_TEST_EQUAL((sectionid_t)0, posIter->GetSectionId());
        INDEXLIB_TEST_EQUAL((section_len_t)3, posIter->GetSectionLength());
        INDEXLIB_TEST_EQUAL((section_weight_t)7, posIter->GetSectionWeight());
        INDEXLIB_TEST_EQUAL((fieldid_t)0, posIter->GetFieldId());

        INDEXLIB_TEST_EQUAL((pos_t)4, posIter->SeekPosition(0));
        if (optionFlag & of_position_payload) {
            INDEXLIB_TEST_EQUAL((pospayload_t)6, posIter->GetPosPayload());
        } else {
            INDEXLIB_TEST_EQUAL((pospayload_t)INVALID_POS_PAYLOAD, posIter->GetPosPayload());
        }
        INDEXLIB_TEST_EQUAL((sectionid_t)0, posIter->GetSectionId());
        INDEXLIB_TEST_EQUAL((section_len_t)9, posIter->GetSectionLength());
        INDEXLIB_TEST_EQUAL((section_weight_t)4, posIter->GetSectionWeight());
        INDEXLIB_TEST_EQUAL((fieldid_t)1, posIter->GetFieldId());

        // test the second cloned iter
        {
            TermMatchData termMathData2;
            iter.Unpack(termMathData2);
            if (optionFlag & of_doc_payload) {
                INDEXLIB_TEST_EQUAL(3, termMathData2.GetDocPayload());
            }
            std::shared_ptr<InDocPositionIterator> posIter2 = termMathData2.GetInDocPositionIterator();

            INDEXLIB_TEST_EQUAL((pos_t)1, posIter2->SeekPosition(0));
            if (optionFlag & of_position_payload) {
                INDEXLIB_TEST_EQUAL((pospayload_t)3, posIter2->GetPosPayload());
            } else {
                INDEXLIB_TEST_EQUAL((pospayload_t)INVALID_POS_PAYLOAD, posIter->GetPosPayload());
            }

            INDEXLIB_TEST_EQUAL((pos_t)4, posIter2->SeekPosition(3));
            if (optionFlag & of_position_payload) {
                INDEXLIB_TEST_EQUAL((pospayload_t)6, posIter2->GetPosPayload());
            } else {
                INDEXLIB_TEST_EQUAL((pospayload_t)INVALID_POS_PAYLOAD, posIter->GetPosPayload());
            }
        }

        INDEXLIB_TEST_EQUAL((pos_t)8, posIter->SeekPosition(5));
        if (optionFlag & of_position_payload) {
            INDEXLIB_TEST_EQUAL((pospayload_t)1, posIter->GetPosPayload());
        } else {
            INDEXLIB_TEST_EQUAL((pospayload_t)INVALID_POS_PAYLOAD, posIter->GetPosPayload());
        }
        INDEXLIB_TEST_EQUAL((sectionid_t)0, posIter->GetSectionId());
        INDEXLIB_TEST_EQUAL((section_len_t)9, posIter->GetSectionLength());
        INDEXLIB_TEST_EQUAL((section_weight_t)4, posIter->GetSectionWeight());
        INDEXLIB_TEST_EQUAL((fieldid_t)1, posIter->GetFieldId());

        INDEXLIB_TEST_EQUAL((pos_t)12, posIter->SeekPosition(10));
        if (optionFlag & of_position_payload) {
            INDEXLIB_TEST_EQUAL((pospayload_t)1, posIter->GetPosPayload());
        } else {
            INDEXLIB_TEST_EQUAL((pospayload_t)INVALID_POS_PAYLOAD, posIter->GetPosPayload());
        }
        INDEXLIB_TEST_EQUAL((sectionid_t)0, posIter->GetSectionId());
        INDEXLIB_TEST_EQUAL((section_len_t)5, posIter->GetSectionLength());
        INDEXLIB_TEST_EQUAL((section_weight_t)3, posIter->GetSectionWeight());
        INDEXLIB_TEST_EQUAL((fieldid_t)2, posIter->GetFieldId());

        INDEXLIB_TEST_EQUAL((pos_t)17, posIter->SeekPosition(13));
        if (optionFlag & of_position_payload) {
            INDEXLIB_TEST_EQUAL((pospayload_t)8, posIter->GetPosPayload());
        } else {
            INDEXLIB_TEST_EQUAL((pospayload_t)INVALID_POS_PAYLOAD, posIter->GetPosPayload());
        }
        INDEXLIB_TEST_EQUAL((sectionid_t)1, posIter->GetSectionId());
        INDEXLIB_TEST_EQUAL((section_len_t)6, posIter->GetSectionLength());
        INDEXLIB_TEST_EQUAL((section_weight_t)7, posIter->GetSectionWeight());
        INDEXLIB_TEST_EQUAL((fieldid_t)2, posIter->GetFieldId());

        INDEXLIB_TEST_EQUAL((pos_t)INVALID_POSITION, posIter->SeekPosition(999));

        // test second document
        INDEXLIB_TEST_EQUAL(4, iter.SeekDoc(2));

        iter.Unpack(termMathData);
        if (optionFlag & of_doc_payload) {
            INDEXLIB_TEST_EQUAL(1, termMathData.GetDocPayload());
        }
        posIter = termMathData.GetInDocPositionIterator();

        INDEXLIB_TEST_EQUAL((pos_t)4, posIter->SeekPosition(1));
        if (optionFlag & of_position_payload) {
            INDEXLIB_TEST_EQUAL((pospayload_t)2, posIter->GetPosPayload());
        } else {
            INDEXLIB_TEST_EQUAL((pospayload_t)INVALID_POS_PAYLOAD, posIter->GetPosPayload());
        }
        INDEXLIB_TEST_EQUAL((sectionid_t)1, posIter->GetSectionId());
        INDEXLIB_TEST_EQUAL((section_len_t)2, posIter->GetSectionLength());
        INDEXLIB_TEST_EQUAL((section_weight_t)2, posIter->GetSectionWeight());
        INDEXLIB_TEST_EQUAL((fieldid_t)1, posIter->GetFieldId());

        INDEXLIB_TEST_EQUAL((pos_t)17, posIter->SeekPosition(5));
        if (optionFlag & of_position_payload) {
            INDEXLIB_TEST_EQUAL((pospayload_t)1, posIter->GetPosPayload());
        } else {
            INDEXLIB_TEST_EQUAL((pospayload_t)INVALID_POS_PAYLOAD, posIter->GetPosPayload());
        }
        INDEXLIB_TEST_EQUAL((sectionid_t)1, posIter->GetSectionId());
        INDEXLIB_TEST_EQUAL((section_len_t)9, posIter->GetSectionLength());
        INDEXLIB_TEST_EQUAL((section_weight_t)8, posIter->GetSectionWeight());
        INDEXLIB_TEST_EQUAL((fieldid_t)2, posIter->GetFieldId());
    } else {
        INDEXLIB_TEST_TRUE(NULL == posIter);
    }
}

void NormalInDocPositionIteratorTest::TestCaseForRepeatInit()
{
    optionflag_t optionFlag = of_position_list;
    PositionListFormatOption posListFormatOption(optionFlag);

    Pool byteSlicePool(10240);
    RecyclePool bufferPool(10240);

    PositionListEncoder positionEncoder(posListFormatOption, &byteSlicePool, &bufferPool);

    for (size_t i = 0; i < 100; ++i) {
        positionEncoder.AddPosition(i, 0);
    }
    positionEncoder.EndDocument();

    for (size_t i = 0; i < 50; ++i) {
        positionEncoder.AddPosition(i, 0);
    }
    positionEncoder.EndDocument();
    positionEncoder.Flush();

    const ByteSliceList* byteSliceList = positionEncoder.GetPositionList();
    NormalInDocPositionIterator posIter(optionFlag);
    NormalInDocState state(PFOR_DELTA_COMPRESS_MODE, optionFlag);

    PositionListSegmentDecoder posSegDecoder(optionFlag, &byteSlicePool);
    posSegDecoder.Init(byteSliceList, 150, 0, PFOR_DELTA_COMPRESS_MODE, &state);

    state.SetRecordOffset(0);
    state.SetOffsetInRecord(100);
    state.SetPositionListSegmentDecoder(&posSegDecoder);
    state.SetTermFreq(50);

    posIter.Init(state);
    for (size_t i = 0; i < 50; ++i) {
        INDEXLIB_TEST_EQUAL((pos_t)i, posIter.SeekPosition(i));
    }

    state.SetRecordOffset(0);
    state.SetOffsetInRecord(0);
    state.SetPositionListSegmentDecoder(&posSegDecoder);
    state.SetTermFreq(100);

    posIter.Init(state);
    for (size_t i = 0; i < 100; ++i) {
        INDEXLIB_TEST_EQUAL((pos_t)i, posIter.SeekPosition(i));
    }
}

LegacySectionAttributeReaderImplPtr NormalInDocPositionIteratorTest::PrepareSectionReader()
{
    LegacySectionAttributeReaderImplPtr reader(new LegacySectionAttributeReaderImpl);
    PartitionDataPtr partitionData = IndexTestUtil::CreatePartitionData(GET_FILE_SYSTEM(), 1);
    reader->Open(mIndexConfig, partitionData);
    return reader;
}
}} // namespace indexlib::index
