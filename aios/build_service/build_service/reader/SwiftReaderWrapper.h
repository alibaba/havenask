#ifndef ISEARCH_BS_SWIFTREADERWRAPPER_H
#define ISEARCH_BS_SWIFTREADERWRAPPER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <swift/client/SwiftReader.h>

namespace build_service {
namespace reader {

class SwiftReaderWrapper
{
public:
    SwiftReaderWrapper(SWIFT_NS(client)::SwiftReader* reader);
    ~SwiftReaderWrapper();
private:
    SwiftReaderWrapper(const SwiftReaderWrapper &);
    SwiftReaderWrapper& operator=(const SwiftReaderWrapper &);

public:
    SWIFT_NS(protocol)::ErrorCode seekByTimestamp(int64_t timestamp, bool force = false);
    void setTimestampLimit(int64_t timestampLimit, int64_t &acceptTimestamp);

    SWIFT_NS(protocol)::ErrorCode read(int64_t &timeStamp, SWIFT_NS(protocol)::Message &msg,
                                       int64_t timeout = 3 * 1000000);  // 3s
    SWIFT_NS(protocol)::ErrorCode
        read(int64_t &timeStamp, SWIFT_NS(protocol)::Messages &msgs, size_t maxMessageCount,
             int64_t timeout = 3 * 1000000);  // 3 s

    SWIFT_NS(client)::SwiftPartitionStatus getSwiftPartitionStatus() {
        return _reader->getSwiftPartitionStatus();
    }

private:
    SWIFT_NS(client)::SwiftReader *_reader;
    SWIFT_NS(protocol)::Messages _swiftMessages;
    int32_t _swiftCursor;
    int64_t _nextBufferTimestamp;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SwiftReaderWrapper);

}
}

#endif //ISEARCH_BS_SWIFTREADERWRAPPER_H
