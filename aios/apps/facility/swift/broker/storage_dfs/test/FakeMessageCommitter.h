#pragma once

#include <stdint.h>

#include "autil/Log.h"
#include "swift/broker/storage_dfs/MessageCommitter.h"
#include "swift/common/Common.h"
#include "swift/protocol/ErrCode.pb.h"

namespace swift {
namespace common {
class MemoryMessage;
} // namespace common
namespace config {
class PartitionConfig;
} // namespace config
namespace monitor {
class BrokerMetricsReporter;
} // namespace monitor
namespace protocol {
class PartitionId;
} // namespace protocol
namespace storage {
class FileManager;
} // namespace storage
} // namespace swift

namespace swift {
namespace storage {

class FakeMessageCommitter : public MessageCommitter {
public:
    FakeMessageCommitter(const protocol::PartitionId &partitionId,
                         const config::PartitionConfig &config,
                         FileManager *fileManager,
                         monitor::BrokerMetricsReporter *amonMetricsReporter);
    ~FakeMessageCommitter();

private:
    FakeMessageCommitter(const FakeMessageCommitter &);
    FakeMessageCommitter &operator=(const FakeMessageCommitter &);

public:
    protocol::ErrorCode appendMsgForDataFile(const common::MemoryMessage &memoryMessage, int64_t writeLen);
    protocol::ErrorCode appendMsgMetaForMetaFile(const common::MemoryMessage &memoryMessage, int64_t writeLen);

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(FakeMessageCommitter);

} // namespace storage
} // namespace swift
