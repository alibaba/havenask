#include "indexlib/index/normal/inverted_index/format/doc_list_encoder.h"
#include "indexlib/index/normal/inverted_index/perf_test/in_mem_doc_list_decoder_perf_unittest.h"
#include "indexlib/file_system/buffered_file_writer.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InMemDocListDecoderPerfTest);

InMemDocListDecoderPerfTest::InMemDocListDecoderPerfTest()
    : mByteSlicePool(1024 * 1024 * 10)
    , mBufferPool(1024 * 1024 * 10)
    , mGlobalDocId(0)
    , mIsDumped(false)
{
}

InMemDocListDecoderPerfTest::~InMemDocListDecoderPerfTest()
{
}

void InMemDocListDecoderPerfTest::CaseSetUp()
{
    DocListFormatOption option(OPTION_FLAG_NONE);
    mEncoder.reset(new DocListEncoder(option, &mSimplePool, &mByteSlicePool, &mBufferPool));
    mGlobalDocId = 0;
    mIsDumped = false;
    mDir = GET_TEST_DATA_PATH();
}

void InMemDocListDecoderPerfTest::CaseTearDown()
{
}

void InMemDocListDecoderPerfTest::DoWrite()
{
    assert(mEncoder);
    mGlobalDocId = 0;
    size_t idx = 0;
    while (!IsFinished())
    {
        mEncoder->EndDocument(mGlobalDocId, mGlobalDocId);
        idx++;
        mGlobalDocId += GetDocIdDeltaByIdx(idx);
        if (mGlobalDocId >= (size_t)std::numeric_limits<docid_t>::max())
        {
            break;
        }
    }

    string filePath = mDir + "dump_file";
    BufferedFileWriterPtr fileWriter(new BufferedFileWriter());
    fileWriter->Open(filePath);
    mEncoder->Dump(fileWriter);
    fileWriter->Close();

    sleep(2);
    mIsDumped = true;
}

void InMemDocListDecoderPerfTest::DoRead(int* errCode)
{
    autil::mem_pool::Pool sessionPool(1024 * 1024 * 10);
    size_t readCount = 0;
    while (!IsFinished() || !mIsDumped)
    {
        readCount++;
        size_t expectLastDocId = mGlobalDocId;
        InMemDocListDecoder* decoder = 
            mEncoder->GetInMemDocListDecoder(&sessionPool);
        docid_t buffer[MAX_DOC_PER_RECORD];
        size_t idx = 0;
        size_t currentDocid = 0;
        while (true)
        {
            docid_t firstDocId;
            docid_t lastDocId;
            ttf_t currentTTF;
            if (!decoder->DecodeDocBuffer(currentDocid, buffer, firstDocId,
                            lastDocId, currentTTF))
            {
                break;
            }
            if (firstDocId != (docid_t)currentDocid)
            {
                *errCode = -1;
                return;
            }
            // cout << "currentDocid " << currentDocid << " lastDocId" << lastDocId << endl;
            // cout << "idx " << idx <<endl;
            size_t i = 0;
            while (currentDocid <= (size_t)lastDocId)
            {
                if (buffer[i] != GetDocIdDeltaByIdx(idx))
                {
                    cout << "read delta is " << buffer[i]
                         << " i is " << i << " last docid is " << lastDocId << " cur docid " << currentDocid
                         << "first docid " << firstDocId
                         << " expect delta is " << GetDocIdDeltaByIdx(idx) << endl;
                    *errCode = -2;
                    return;
                }
                i++;
                idx++;
                currentDocid += GetDocIdDeltaByIdx(idx);
            }
        }
        if (currentDocid < expectLastDocId)
        {
            cout << "read last docid is " << currentDocid
                 << " expect last docid " << expectLastDocId << endl;
            *errCode = -3;
            return;
        }
    }
    // cout << readCount << endl;
}

void InMemDocListDecoderPerfTest::TestGetInMemDocListDecoder()
{
    DoMultiThreadTest(10, 5);
}

IE_NAMESPACE_END(index);

