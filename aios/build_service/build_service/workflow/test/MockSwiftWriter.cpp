#include "build_service/workflow/test/MockSwiftWriter.h"
#include <functional>

using namespace std;

SWIFT_USE_NAMESPACE(protocol);
namespace build_service {
namespace workflow {

MockSwiftWriter::MockSwiftWriter() {
    ON_CALL(*this, write(_)).WillByDefault(
            Invoke(bind(&MockSwiftWriter::writeForTest, this, placeholders::_1)));
    ON_CALL(*this, getCommittedCheckpointId()).WillByDefault(
            Invoke(bind(&MockSwiftWriter::getCheckpointForTest, this)));
    ON_CALL(*this, waitFinished(_)).WillByDefault(Return(ERROR_NONE));
    ON_CALL(*this, getWriterMetrics(_,_,_)).WillByDefault(
            Invoke(bind(&MockSwiftWriter::getWriterMetricsTest,
                        this, placeholders::_1, placeholders::_2, placeholders::_3)));
    _checkpoint = -1;
    _writeMetrics.uncommittedMsgCount = 1;
}

MockSwiftWriter::~MockSwiftWriter() {
}

ErrorCode MockSwiftWriter::writeForTest(MessageInfo &messageInfo) {
    _messageInfos.push_back(messageInfo);
    return ERROR_NONE;
}

void MockSwiftWriter::getWriterMetricsTest(const std::string&, const std::string&,
        WriterMetrics& metrics) {
    metrics = _writeMetrics;
}

int64_t MockSwiftWriter::getCheckpointForTest() const {
    return _checkpoint;
}

}
}
