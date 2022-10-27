#include "indexlib/index/normal/inverted_index/perf_test/in_mem_bitmap_posting_iterator_perf_unittest.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include "indexlib/util/object_pool.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/single_bitmap_posting_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/position_iterator_typed.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InMemBitmapPostingIteratorPerfTest);

InMemBitmapPostingIteratorPerfTest::InMemBitmapPostingIteratorPerfTest()
    : mByteSlicePool(1024 * 1024 * 10)
    , mGlobalDocId(0)
    , mIsDumped(false)
{
}

InMemBitmapPostingIteratorPerfTest::~InMemBitmapPostingIteratorPerfTest()
{
}

void InMemBitmapPostingIteratorPerfTest::CaseSetUp()
{
    mBitmapPostingWriter.reset(new BitmapPostingWriter(&mByteSlicePool));
    mGlobalDocId = 0;
    mIsDumped = false;
    mDir = GET_TEST_DATA_PATH();
}

void InMemBitmapPostingIteratorPerfTest::CaseTearDown()
{
}

void InMemBitmapPostingIteratorPerfTest::DoWrite()
{
    assert(mBitmapPostingWriter);
    size_t idx = 0;
    mGlobalDocId = 0;
    while (!IsFinished())
    {
        size_t nextDocId = mGlobalDocId + GetDocIdDeltaByIdx(idx);
        if (nextDocId >= (size_t)std::numeric_limits<docid_t>::max())
        {
            break;
        }
        
        mBitmapPostingWriter->EndDocument(nextDocId, mGlobalDocId);
        MEMORY_BARRIER();
        mGlobalDocId = nextDocId;
        idx++;
    }

    string filePath = mDir + "dump_file";
    FileWriterPtr fileWriter(new BufferedFileWriter());
    fileWriter->Open(filePath);
    mBitmapPostingWriter->Dump(fileWriter);
    fileWriter->Close();

    sleep(2);
    mIsDumped = true;
}

void InMemBitmapPostingIteratorPerfTest::DoRead(int* errCode)
{
    autil::mem_pool::Pool sessionPool(1024 * 1024 * 10);
    typedef PositionIteratorTyped<SingleBitmapPostingIterator>::StateType Statetype;
    ObjectPool<Statetype> statePool;

    size_t readCount = 0;
    while (!IsFinished() || !mIsDumped)
    {
        readCount++;
        size_t expectLastDocId = mGlobalDocId;
        if (expectLastDocId == 0)
        {
            continue;
        }

        SingleBitmapPostingIterator iter;
        iter.Init(mBitmapPostingWriter.get(), &statePool);

        size_t idx = 0;
        docid_t lastDocid = 0;
        docid_t currentDocid = 0;
        while (true)
        {
            currentDocid = currentDocid + GetDocIdDeltaByIdx(idx);
            if ((size_t)currentDocid > expectLastDocId)
            {
                break;
            }

            for (docid_t i = (lastDocid + 1); i < currentDocid; ++i)
            {
                if (iter.Test(i))
                {
                    *errCode = -1;
                    return;
                }
            }

            if (!iter.Test(currentDocid))
            {
                *errCode = -2;
                return;
            }
            lastDocid = currentDocid;
            idx++;
        }
    }
    // cout << readCount << endl;
}

void InMemBitmapPostingIteratorPerfTest::TestGetInMemBitmapPostingIterator()
{
    DoMultiThreadTest(10, 5);
}

IE_NAMESPACE_END(index);

