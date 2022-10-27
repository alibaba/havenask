#include "indexlib/index/normal/inverted_index/test/buffered_index_decoder_unittest.h"

using namespace std;


IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, BufferedIndexDecoderTest);

BufferedIndexDecoderTest::BufferedIndexDecoderTest()
{
    PrepareSegmentPostings();
}

BufferedIndexDecoderTest::~BufferedIndexDecoderTest()
{
}

void BufferedIndexDecoderTest::CaseSetUp()
{
}

void BufferedIndexDecoderTest::CaseTearDown()
{
}

void BufferedIndexDecoderTest::TestGetSegmentBaseDocId()
{
    BufferedIndexDecoder decoder(NULL, NULL);
    decoder.mSegPostings = mSegPostings;

    for (size_t i = 0; i < 5; i++)
    {
        docid_t baseDocId = decoder.GetSegmentBaseDocId((uint32_t)i);
        ASSERT_EQ((docid_t)i*1000, baseDocId);
    }
}

void BufferedIndexDecoderTest::TestLocateSegment()
{
    BufferedIndexDecoder decoder(NULL, NULL);
    decoder.mSegPostings = mSegPostings;

    {
        uint32_t segCursor = decoder.LocateSegment(0, INVALID_DOCID);
        ASSERT_EQ((uint32_t)0, segCursor);
    }

    {
        uint32_t segCursor = decoder.LocateSegment(1, 3000);
        ASSERT_EQ((uint32_t)3, segCursor);
    }

    {
        uint32_t segCursor = decoder.LocateSegment(4, 4001);
        ASSERT_EQ((uint32_t)4, segCursor);
    }

    {
        uint32_t segCursor = decoder.LocateSegment(5, 4999);
        ASSERT_EQ((uint32_t)5, segCursor);
    }
}

void BufferedIndexDecoderTest::PrepareSegmentPostings()
{
    mSegPostings.reset(new SegmentPostingVector);
    for (size_t i = 0; i < 5; i++)
    {
        SegmentPosting segPosting(mFormatOption);
        segPosting.SetBaseDocId((docid_t)i * 1000);
        mSegPostings->push_back(segPosting);
    }
}

IE_NAMESPACE_END(index);

