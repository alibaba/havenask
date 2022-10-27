#include "indexlib/index/normal/inverted_index/test/posting_writer_impl_unittest.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include "indexlib/file_system/in_mem_file_node.h"
#include "indexlib/index/normal/inverted_index/format/short_list_optimize_util.h"
#include "indexlib/file_system/in_mem_file_node_creator.h"
#include "indexlib/index/normal/inverted_index/format/term_meta_dumper.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace std::tr1;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PostingWriterImplTest);

PostingWriterImplTest::PostingWriterImplTest()
{
    mByteSlicePool = new Pool(&mAllocator, 10240);
    mBufferPool = new RecyclePool(&mAllocator, 10240);
}

PostingWriterImplTest::~PostingWriterImplTest()
{
    delete mByteSlicePool;
    delete mBufferPool;
}

void PostingWriterImplTest::SetUp()
{
    mRootDir = FileSystemWrapper::JoinPath(TEST_DATA_PATH, "posting_writer_impl_test");
    if (FileSystemWrapper::IsExist(mRootDir))
    {
        FileSystemWrapper::DeleteDir(mRootDir);
    }
    FileSystemWrapper::MkDir(mRootDir);
    mFile = FileSystemWrapper::JoinPath(mRootDir, "file");
}

void PostingWriterImplTest::TearDown()
{
    FileSystemWrapper::DeleteDir(mRootDir);
}

void PostingWriterImplTest::TestSimpleProcess()
{
    optionflag_t optionFlag = EXPACK_OPTION_FLAG_ALL;
    TestDump(1000, optionFlag, it_expack);
}

void PostingWriterImplTest::TestPostingWriterWithoutPositionList()
{
    optionflag_t optionFlag = NO_POSITION_LIST;
    
    //for short list mode
    TestDump(1, optionFlag, it_pack);

    //for compress mode
    TestDump(1000, optionFlag, it_pack);
}

void PostingWriterImplTest::TestPostingWriterWithPositionList()
{
    optionflag_t optionFlag = OPTION_FLAG_ALL;
    
    //for short list mode
    TestDump(1, optionFlag, it_pack);

    //for compress mode
    TestDump(1000, optionFlag, it_pack);
}

void PostingWriterImplTest::TestPostingWriterWithoutTFList()
{
    optionflag_t optionFlag = NO_TERM_FREQUENCY;
    
    //for short list mode
    TestDump(1, optionFlag, it_pack);

    //for compress mode
    TestDump(1000, optionFlag, it_pack);
}

void PostingWriterImplTest::TestPostingWriterWithTFBitmap()
{
    optionflag_t optionFlag = OPTION_FLAG_ALL | of_tf_bitmap;
    
    //for short list mode
    TestDump(1, optionFlag, it_pack);

    //for compress mode
    TestDump(1000, optionFlag, it_pack);
}

void PostingWriterImplTest::TestPostingWriterWithoutDocPayload()
{
    optionflag_t optionFlag = OPTION_FLAG_ALL & ~of_doc_payload;
    
    //for short list mode
    TestDump(1, optionFlag, it_pack);

    //for compress mode
    TestDump(1000, optionFlag, it_pack);
}

void PostingWriterImplTest::TestPostingWriterWithoutPositionPayload()
{
    optionflag_t optionFlag = OPTION_FLAG_ALL & ~of_position_payload;
    
    //for short list mode
    TestDump(1, optionFlag, it_pack);

    //for compress mode
    TestDump(1000, optionFlag, it_pack);
}

void PostingWriterImplTest::TestPostingWriterWithFieldMap()
{
    optionflag_t optionFlag = EXPACK_OPTION_FLAG_ALL;
    
    //for short list mode
    TestDump(1, optionFlag, it_expack);

    //for compress mode
    TestDump(1000, optionFlag, it_expack);
}

void PostingWriterImplTest::TestCaseForGetDictInlinePostingValue()
{
    {
        //dict inline compress fail: df > 1
        PostingFormatOption formatOption(EXPACK_NO_POSITION_LIST);
        PostingWriterResource writerResource(&mSimplePool, mByteSlicePool,
                mBufferPool, formatOption);
        PostingWriterImpl writer(&writerResource);
        writer.EndDocument(2, 3);
        writer.EndDocument(4, 5);

        uint64_t inlinePostingValue;
        ASSERT_FALSE(writer.GetDictInlinePostingValue(
                        inlinePostingValue));
    }
    {
        //dict inline compress fail: compress fail
        PostingFormatOption formatOption(EXPACK_NO_POSITION_LIST);
        PostingWriterResource writerResource(&mSimplePool, mByteSlicePool,
                mBufferPool, formatOption);
        PostingWriterImpl writer(&writerResource);

        //30000 0000 > 2^28 need 5 bytes to compress 
        writer.SetTermPayload(300000000);
        writer.EndDocument(300000000, 3);

        uint64_t inlinePostingValue;
        ASSERT_FALSE(writer.GetDictInlinePostingValue(
                        inlinePostingValue));

    }
    {
        //normal dict inline compress
        PostingFormatOption formatOption(EXPACK_NO_POSITION_LIST);
        PostingWriterResource writerResource(&mSimplePool, mByteSlicePool,
                mBufferPool, formatOption);
        PostingWriterImpl writer(&writerResource);

        //30000 0000 > 2^28 need 5 bytes to compress 
        writer.AddPosition(0, 0, 0);
        writer.SetTermPayload(1);
        writer.EndDocument(2, 3);

        uint64_t inlinePostingValue;
        ASSERT_TRUE(writer.GetDictInlinePostingValue(
                        inlinePostingValue));
    }
}

void PostingWriterImplTest::Check(
        PostingFormatOption& postingFormatOption, Answer& answer,
        BufferedPostingIterator& iter, uint8_t compressMode)
{
    DocListFormatOption docListOption
        = postingFormatOption.GetDocListFormatOption();
    PositionListFormatOption positionListOption
        = postingFormatOption.GetPosListFormatOption();

    for (uint32_t i = 0; i < answer.docIdList.size(); ++i)
    {
        CheckDocList(docListOption, answer, i, iter);
        AnswerInDoc& answerInDoc = answer.inDocAnswers[answer.docIdList[i]];
        CheckPositionList(positionListOption, iter, answerInDoc);
    }
}

void PostingWriterImplTest::CheckPositionList(
        PositionListFormatOption positionListOption,
        BufferedPostingIterator& iter,
        AnswerInDoc& answerInDoc)
{
    if (positionListOption.HasPositionList())
    {
        for (uint32_t j = 0; j < answerInDoc.positionList.size(); ++j)
        {
            pos_t pos = iter.SeekPosition(answerInDoc.positionList[j]);
            INDEXLIB_TEST_EQUAL(answerInDoc.positionList[j], pos);
            if (positionListOption.HasPositionPayload())
            {
                pospayload_t posPayload = iter.GetPosPayload();
                INDEXLIB_TEST_EQUAL(answerInDoc.posPayloadList[j], 
                        posPayload);
            }
        }
    }
}

void PostingWriterImplTest::CheckDocList(
        DocListFormatOption docListOption,
        Answer& answer, size_t answerCursor,
        BufferedPostingIterator& iter)
{
    docid_t id = iter.SeekDoc(answer.docIdList[answerCursor]);
    INDEXLIB_TEST_EQUAL(answer.docIdList[answerCursor], id);

    AnswerInDoc& answerInDoc = answer.inDocAnswers[id];
    TermMatchData tmd;
    iter.Unpack(tmd);

    if (docListOption.HasDocPayload())
    {
        INDEXLIB_TEST_EQUAL(answerInDoc.docPayload, iter.GetDocPayload());
        INDEXLIB_TEST_EQUAL(answerInDoc.docPayload, tmd.GetDocPayload());
    }

    if (docListOption.HasTfList())
    {
        INDEXLIB_TEST_EQUAL((tf_t)answer.tfList[answerCursor], tmd.GetTermFreq());
    }

    if (docListOption.HasFieldMap())
    {
        INDEXLIB_TEST_EQUAL(answerInDoc.fieldMap, tmd.GetFieldMap());
    }
    tmd.FreeInDocPositionState();
}

void PostingWriterImplTest::TestDump(uint32_t docCount,
                                     optionflag_t optionFlag,
                                     IndexType indexType)
{
    IndexDocumentMaker::ResetDir(mRootDir);

    PackageIndexConfigPtr indexConfig(new PackageIndexConfig("package", indexType));
    indexConfig->SetOptionFlag(optionFlag);
    IndexFormatOption indexFormatOption;
    indexFormatOption.Init(indexConfig);
    PostingFormatOption postingOption = indexFormatOption.GetPostingFormatOption();

    PostingWriterResource writerResource(&mSimplePool, mByteSlicePool,
                mBufferPool, postingOption);
    PostingWriterImplPtr writer(new PostingWriterImpl(&writerResource, true));
    //check PositionBitmapWriter
    if ((optionFlag & of_term_frequency) 
        && (optionFlag & of_tf_bitmap))
    {
        INDEXLIB_TEST_TRUE(writer->GetPositionBitmapWriter() != NULL);
    }

    Answer answer;
    PostingWriterHelper::CreateAnswer(docCount, answer);
    PostingWriterHelper::Build(writer, answer);
    writer->EndSegment();

    string fileName = FileSystemWrapper::JoinPath(mRootDir, "posting");
    file_system::FileWriterPtr file(
            new file_system::BufferedFileWriter);
    file->Open(fileName);

    TermMetaPtr termMeta(new TermMeta());
    TermMetaDumper tmDumper(postingOption);
    termMeta->SetDocFreq(writer->GetDF());
    termMeta->SetTotalTermFreq(writer->GetTotalTF());
    uint8_t compressMode = ShortListOptimizeUtil::GetCompressMode(
            writer->GetDF(), writer->GetTotalTF(),
            postingOption.GetDocListCompressMode());
    termMeta->SetPayload(100);
    tmDumper.Dump(file, *termMeta);
    writer->Dump(file);
    file->Close();
    file_system::InMemFileNodePtr fileReader(file_system::InMemFileNodeCreator::Create());
    fileReader->Open(fileName, file_system::FSOT_IN_MEM);
    fileReader->Populate();
    uint32_t postingLength = tmDumper.CalculateStoreSize(*termMeta) + writer->GetDumpLength();
    ByteSliceListPtr sliceListPtr(fileReader->Read(postingLength, 0));

    BufferedPostingIterator iter(postingOption, NULL);
    SegmentPosting segPosting(postingOption);
    segPosting.Init(compressMode, sliceListPtr, 0, 
                    SegmentInfo());
    SegmentPostingVectorPtr segPostings(new SegmentPostingVector);
    segPostings->push_back(segPosting);
    FakeSectionAttributeReader fakeReader;
    iter.Init(segPostings, &fakeReader, 5);

    Check(postingOption, answer, iter, compressMode);
    fileReader->Close();
}

void PostingWriterImplTest::TestCaseForInMemPostingDecoder()
{
    IndexDocumentMaker::ResetDir(mRootDir);

    optionflag_t optionFlag = of_none;

    PackageIndexConfigPtr indexConfig(new PackageIndexConfig("package", it_pack));
    indexConfig->SetOptionFlag(optionFlag);
    IndexFormatOption indexFormatOption;
    indexFormatOption.Init(indexConfig);
    PostingFormatOption postingOption = indexFormatOption.GetPostingFormatOption();
    PostingWriterResource writerResource(&mSimplePool, mByteSlicePool,
                mBufferPool, postingOption);
    PostingWriterImplPtr writer(new PostingWriterImpl(&writerResource));

    uint32_t docCount = 128;
    Answer answer;
    PostingWriterHelper::CreateAnswer(docCount, answer);
    PostingWriterHelper::Build(writer, answer);

    SegmentPosting segPosting(postingOption);
    segPosting.Init(0, 0, writer.get());
    segPosting.SetBaseDocId(0);

    SegmentPostingVectorPtr segPostings(new SegmentPostingVector);
    segPostings->push_back(segPosting);

    BufferedPostingIterator iter(postingOption, NULL);
    iter.Init(segPostings, NULL, 5);

    uint8_t compressMode = ShortListOptimizeUtil::GetCompressMode(
            writer->GetDF(), writer->GetTotalTF(),
            postingOption.GetDocListCompressMode());
    Check(postingOption, answer, iter, compressMode);
}

void PostingWriterImplTest::TestCaseForPostingFormat()
{
    PostingFormatOption postingFormatOption(EXPACK_OPTION_FLAG_ALL);

    PostingWriterResource writerResource(&mSimplePool, mByteSlicePool,
                mBufferPool, postingFormatOption);

    PostingWriterImpl postingWriterImpl(&writerResource);

    ASSERT_TRUE(postingWriterImpl.mDocListEncoder != NULL);
    ASSERT_EQ(postingWriterImpl.mDocListEncoder->GetDocListFormat(),
              writerResource.postingFormat->GetDocListFormat());
    ASSERT_EQ(postingWriterImpl.mPositionListEncoder->GetPositionListFormat(),
              writerResource.postingFormat->GetPositionListFormat());
}

IE_NAMESPACE_END(index);

