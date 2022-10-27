#ifndef ISEARCH_BS_MOCKSWIFTWRITER_H
#define ISEARCH_BS_MOCKSWIFTWRITER_H

#include "build_service/test/unittest.h"
#include "build_service/util/Log.h"
#include "build_service/test/test.h"
#include <swift/client/SwiftWriter.h>

namespace build_service {
namespace workflow {

class MockSwiftWriter : public SWIFT_NS(client)::SwiftWriter
{
public:
    typedef SWIFT_NS(protocol)::ErrorCode ErrorCode;
    typedef SWIFT_NS(client)::MessageInfo MessageInfo;
    typedef SWIFT_NS(client)::WriterMetrics WriterMetrics;
public:
    MockSwiftWriter();
    ~MockSwiftWriter();
public:
    MOCK_METHOD1(write, ErrorCode(MessageInfo&));
    MOCK_CONST_METHOD0(getCommittedCheckpointId, int64_t());
    MOCK_CONST_METHOD1(getLastRefreshTime, int64_t(uint32_t));
    MOCK_METHOD1(waitFinished, ErrorCode(int64_t));
    MOCK_METHOD1(getPartitionIdByHashStr, std::pair<int32_t, uint16_t>(const std::string &hashStr));
    MOCK_METHOD3(getPartitionIdByHashStr,
                 std::pair<int32_t,uint16_t> (const std::string&, const std::string&, const std::string&));
    MOCK_METHOD3(getWriterMetrics, void(const std::string&, const std::string&, WriterMetrics&));
    MOCK_CONST_METHOD0(isSyncWriter, bool());
    MOCK_METHOD2(clearBuffer, bool(int64_t &,  int64_t &));

    MOCK_METHOD0(isFinished, bool());
    MOCK_METHOD0(isAllSent, bool());
public:
    // delegate function
    ErrorCode writeForTest(MessageInfo &messageInfo);
    int64_t getCheckpointForTest() const;
    void getWriterMetricsTest(const std::string&, const std::string&,
                              WriterMetrics& metrics);
public:
    const std::vector<MessageInfo> &getMessages() const {
        return _messageInfos;
    }
    void setCheckpoint(int64_t checkpoint) {
        _checkpoint = checkpoint;
    }

public:
    WriterMetrics _writeMetrics;
private:
    std::vector<MessageInfo> _messageInfos;
    int64_t _checkpoint;
}; 

typedef ::testing::StrictMock<MockSwiftWriter> StrictMockSwiftWriter;
typedef ::testing::NiceMock<MockSwiftWriter> NiceMockSwiftWriter;

}
}

#endif //ISEARCH_BS_MOCKSWIFTWRITER_H
