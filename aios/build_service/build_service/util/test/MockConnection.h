#ifndef ISEARCH_BS_MOCKCONNECTION_H
#define ISEARCH_BS_MOCKCONNECTION_H

#include "build_service/test/unittest.h"
#include "build_service/test/test.h"
#include "build_service/util/Log.h"
#include <anet/connection.h>

namespace anet {

class MockIOComponent : public IOComponent {
public:
    MockIOComponent() : IOComponent(new Transport(), new Socket()) {}
    ~MockIOComponent() {
        delete _owner;
    }
public:
    MOCK_METHOD1(init, bool(bool));
    MOCK_METHOD0(handleWriteEvent, bool());
    MOCK_METHOD0(handleReadEvent, bool());
    MOCK_METHOD0(handleErrorEvent, bool());
    MOCK_METHOD1(checkTimeout, bool(int64_t));
};

class MockPacketStreamer : public IPacketStreamer {
public:
    MockPacketStreamer() : IPacketStreamer(NULL) {}
    ~MockPacketStreamer() {}
public:
    MOCK_METHOD0(createContext, StreamingContext*());
    MOCK_METHOD3(getPacketInfo, bool(DataBuffer *, PacketHeader *, bool *));
    MOCK_METHOD2(decode, Packet*(DataBuffer *, PacketHeader *));
    MOCK_METHOD2(encode, bool(Packet *, DataBuffer *));
    MOCK_METHOD2(processData, bool(DataBuffer *, StreamingContext *));
};
typedef ::testing::StrictMock<MockPacketStreamer> StrictMockPacketStreamer;
typedef ::testing::NiceMock<MockPacketStreamer> NiceMockPacketStreamer;

class MockConnection : public Connection
{
public:
    MockConnection(IPacketStreamer *streamer) : Connection(NULL, streamer, NULL) {
        setIOComponent(new MockIOComponent());
    }
    ~MockConnection() {}
public:
    MOCK_METHOD0(isClosed, bool());
    MOCK_METHOD0(writeData, bool());
    MOCK_METHOD0(readData, bool());
};

typedef ::testing::StrictMock<MockConnection> StrictMockConnection;
typedef ::testing::NiceMock<MockConnection> NiceMockConnection;

}

#endif //ISEARCH_BS_MOCKCONNECTION_H
