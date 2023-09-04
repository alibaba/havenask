#include "swift/broker/storage/RequestItemQueue.h"

#include <cstddef>
#include <queue>
#include <stdint.h>
#include <vector>

#include "unittest/unittest.h"

namespace swift {
namespace protocol {
class MessageResponse;
class ProductionRequest;
} // namespace protocol
} // namespace swift

using namespace std;
namespace swift {
namespace storage {

class RequestItemQueueTest : public TESTBASE {
public:
    typedef RequestItemInternal<protocol::ProductionRequest, protocol::MessageResponse> WriteRequestItem;
    typedef RequestItemQueue<protocol::ProductionRequest, protocol::MessageResponse> WriteRequestItemQueue;
};

TEST_F(RequestItemQueueTest, testSimpleProcess) {
    WriteRequestItemQueue requestItemQueue(3, 500);
    vector<WriteRequestItem *> requestItemVec;
    requestItemQueue.popRequestItem(requestItemVec, 1);
    EXPECT_EQ((size_t)0, requestItemVec.size());

    WriteRequestItem requestItem1;
    requestItem1.dataSize = 50;
    EXPECT_TRUE(requestItemQueue.pushRequestItem(&requestItem1));
    WriteRequestItem requestItem2;
    requestItem2.dataSize = 100;
    EXPECT_TRUE(requestItemQueue.pushRequestItem(&requestItem2));
    WriteRequestItem requestItem3;
    requestItem3.dataSize = 150;
    EXPECT_TRUE(requestItemQueue.pushRequestItem(&requestItem3));
    EXPECT_EQ((uint64_t)300, requestItemQueue._currDataSize);
    EXPECT_EQ((size_t)3, requestItemQueue._requestItemQueue.size());

    requestItemQueue.popRequestItem(requestItemVec, 0);
    EXPECT_EQ((size_t)0, requestItemVec.size());
    EXPECT_EQ((uint64_t)300, requestItemQueue._currDataSize);

    requestItemVec.clear();
    requestItemQueue.popRequestItem(requestItemVec, 1);
    EXPECT_EQ((size_t)1, requestItemVec.size());
    EXPECT_EQ(&requestItem1, requestItemVec[0]);
    EXPECT_EQ((uint64_t)250, requestItemQueue._currDataSize);
    EXPECT_EQ((size_t)2, requestItemQueue._requestItemQueue.size());

    requestItemVec.clear();
    requestItemQueue.popRequestItem(requestItemVec, 5);
    EXPECT_EQ((size_t)2, requestItemVec.size());
    EXPECT_EQ(&requestItem2, requestItemVec[0]);
    EXPECT_EQ(&requestItem3, requestItemVec[1]);
    EXPECT_EQ((uint64_t)0, requestItemQueue._currDataSize);
    EXPECT_EQ((size_t)0, requestItemQueue._requestItemQueue.size());

    EXPECT_TRUE(requestItemQueue.pushRequestItem(&requestItem1));
    EXPECT_TRUE(requestItemQueue.pushRequestItem(&requestItem2));
    EXPECT_EQ((uint64_t)150, requestItemQueue._currDataSize);
    requestItemVec.clear();
    requestItemQueue.popRequestItem(requestItemVec, 2);
    EXPECT_EQ((size_t)2, requestItemVec.size());
    EXPECT_EQ(&requestItem1, requestItemVec[0]);
    EXPECT_EQ(&requestItem2, requestItemVec[1]);
    EXPECT_EQ((uint64_t)0, requestItemQueue._currDataSize);
}

TEST_F(RequestItemQueueTest, testPushRequestItem) {
    {
        WriteRequestItemQueue requestItemQueue;
        EXPECT_EQ((uint32_t)5000, requestItemQueue._requestItemLimit);
        EXPECT_EQ((uint64_t)(500 * 1024 * 1024), requestItemQueue._dataSizeLimit);

        WriteRequestItem *requestItem1 = new WriteRequestItem();
        requestItem1->dataSize = 300;
        requestItem1->receivedTime = 0;
        EXPECT_TRUE(requestItemQueue.pushRequestItem(requestItem1));
        EXPECT_EQ((size_t)1, requestItemQueue._requestItemQueue.size());
        EXPECT_EQ((uint64_t)300, requestItemQueue._currDataSize);
        EXPECT_TRUE(requestItem1->receivedTime != 0);
    }

    {
        WriteRequestItemQueue requestItemQueue(3, 500);
        EXPECT_EQ((uint32_t)3, requestItemQueue._requestItemLimit);
        EXPECT_EQ((uint64_t)500, requestItemQueue._dataSizeLimit);

        WriteRequestItem *requestItem1 = new WriteRequestItem();
        requestItem1->dataSize = 300;
        EXPECT_TRUE(requestItemQueue.pushRequestItem(requestItem1));
        EXPECT_EQ((size_t)1, requestItemQueue._requestItemQueue.size());
        EXPECT_EQ((uint64_t)300, requestItemQueue._currDataSize);

        WriteRequestItem *requestItem2 = new WriteRequestItem();
        requestItem2->dataSize = 200;
        EXPECT_TRUE(requestItemQueue.pushRequestItem(requestItem2));
        EXPECT_EQ((size_t)2, requestItemQueue._requestItemQueue.size());
        EXPECT_EQ((uint64_t)500, requestItemQueue._currDataSize);

        WriteRequestItem *requestItem3 = new WriteRequestItem();
        requestItem3->dataSize = 200;
        EXPECT_TRUE(!requestItemQueue.pushRequestItem(requestItem3));
        EXPECT_EQ((size_t)2, requestItemQueue._requestItemQueue.size());
        EXPECT_EQ((uint64_t)500, requestItemQueue._currDataSize);
        delete requestItem3;
    }
    {
        WriteRequestItemQueue requestItemQueue(2, 500);
        WriteRequestItem *requestItem1 = new WriteRequestItem();
        requestItem1->dataSize = 100;
        EXPECT_TRUE(requestItemQueue.pushRequestItem(requestItem1));
        EXPECT_EQ((size_t)1, requestItemQueue._requestItemQueue.size());
        EXPECT_EQ((uint64_t)100, requestItemQueue._currDataSize);

        WriteRequestItem *requestItem2 = new WriteRequestItem();
        requestItem2->dataSize = 100;
        EXPECT_TRUE(requestItemQueue.pushRequestItem(requestItem2));
        EXPECT_EQ((size_t)2, requestItemQueue._requestItemQueue.size());
        EXPECT_EQ((uint64_t)200, requestItemQueue._currDataSize);

        WriteRequestItem *requestItem3 = new WriteRequestItem();
        requestItem3->dataSize = 100;
        EXPECT_TRUE(!requestItemQueue.pushRequestItem(requestItem3));
        EXPECT_EQ((size_t)2, requestItemQueue._requestItemQueue.size());
        EXPECT_EQ((uint64_t)200, requestItemQueue._currDataSize);
        delete requestItem3;
    }
}

} // namespace storage
} // namespace swift
