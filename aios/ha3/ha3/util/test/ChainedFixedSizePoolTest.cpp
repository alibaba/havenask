#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/util/ChainedFixedSizePool.h>
#include <autil/Thread.h>

using namespace std;
using namespace autil;

BEGIN_HA3_NAMESPACE(util);

class ChainedFixedSizePoolTest : public TESTBASE {
public:
    ChainedFixedSizePoolTest();
    ~ChainedFixedSizePoolTest();
public:
    void setUp();
    void tearDown();
public:
    void innerTestAllocate();
protected:
    ChainedFixedSizePool *_pool;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(util, ChainedFixedSizePoolTest);


ChainedFixedSizePoolTest::ChainedFixedSizePoolTest() { 
    _pool = NULL;
}

ChainedFixedSizePoolTest::~ChainedFixedSizePoolTest() { 
}

void ChainedFixedSizePoolTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    _pool = new ChainedFixedSizePool;
}

void ChainedFixedSizePoolTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    delete _pool;
}

TEST_F(ChainedFixedSizePoolTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string data;
    data += "2314152452431";
    data.append(1000, 'y');
    data += "abcaaaf12441551142";
    ChainedFixedSizePool pool;
    pool.init(4);
    ChainedBlockItem item = pool.createItem(data.data(), data.length());

    string read;
    read.resize(item.dataSize);
    pool.read(item, const_cast<char*>(read.data()));
    ASSERT_EQ(data, read);

    pool.deallocate(item);
}

TEST_F(ChainedFixedSizePoolTest, testAllocateAndDeallocate) {
    _pool->init(50);
    innerTestAllocate();
}

TEST_F(ChainedFixedSizePoolTest, testMultiThread) {
    _pool->init(10);
    uint32_t threadNum = 5;
    ThreadPtr threads[threadNum];
    for (uint32_t i = 0; i < threadNum; ++i) {
        threads[i] = autil::Thread::createThread(
                std::tr1::bind(&ChainedFixedSizePoolTest::innerTestAllocate, this));
    }
    for (uint32_t i = 0; i < threadNum; ++i) {
        threads[i]->join();
    }
}

void ChainedFixedSizePoolTest::innerTestAllocate() {
    srand(time(0));
    vector<ChainedBlockItem> allLeftItems;
    uint32_t round = 25;
    for (uint32_t i = 0; i < round; ++i) {
        vector<ChainedBlockItem> items;
        uint32_t allocateBlockCount = 100 + rand() % 100;
        for (uint32_t j = 0; j < allocateBlockCount; ++j) {
            uint32_t allocateSize = 1 + rand() % 1000;
            items.push_back(_pool->allocate(allocateSize));
        }
        random_shuffle(items.begin(), items.end());
        uint32_t deallocateBlockCount = rand() % allocateBlockCount;
        for (uint32_t j = 0; j < deallocateBlockCount; ++j) {
            _pool->deallocate(items[j]);
        }
        allLeftItems.insert(allLeftItems.begin(), 
                            items.begin() + deallocateBlockCount,
                            items.end());
    }
    random_shuffle(allLeftItems.begin(), allLeftItems.end());
    for (uint32_t i = 0; i < allLeftItems.size(); ++i) {
        _pool->deallocate(allLeftItems[i]);
    }
} 

END_HA3_NAMESPACE(util);

