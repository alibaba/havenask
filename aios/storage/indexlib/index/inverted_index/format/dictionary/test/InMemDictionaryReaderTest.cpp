#include "indexlib/index/inverted_index/format/dictionary/InMemDictionaryReader.h"

#include "autil/LoopThread.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class InMemDictionaryReaderTest : public TESTBASE
{
};

TEST_F(InMemDictionaryReaderTest, TestSimpleProcess)
{
    autil::mem_pool::Pool pool;
    autil::mem_pool::pool_allocator<int32_t> allocator(&pool);
    InMemDictionaryReader::HashKeyVector hashKeyVector(allocator);
    std::atomic<int32_t> written(0);
    auto writeThread = autil::LoopThread::createLoopThread(
        [hashKeyVectorPtr = &hashKeyVector, &written]() {
            hashKeyVectorPtr->push_back(written.load());
            written++;
        },
        1);
    for (int32_t i = 0; i < 30; ++i) {
        InMemDictionaryReader reader(&hashKeyVector);
        int32_t onSnapshotCnt = written.load();
        std::shared_ptr<DictionaryIterator> iter = reader.CreateIterator();
        index::DictKeyInfo key;
        dictvalue_t value;
        int32_t findCnt = 0;
        while (iter->HasNext()) {
            iter->Next(key, value);
            ASSERT_EQ(dictkey_t(findCnt), key.GetKey());
            findCnt++;
        }
        ASSERT_TRUE(findCnt >= onSnapshotCnt);
    }
    writeThread.reset();
}

} // namespace indexlib::index
