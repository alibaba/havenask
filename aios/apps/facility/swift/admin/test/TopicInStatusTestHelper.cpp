#include "swift/admin/test/TopicInStatusTestHelper.h"

#include "swift/admin/TopicInStatusManager.h"
#include "swift/protocol/Common.pb.h"
#include "unittest/unittest.h"

namespace swift {
namespace admin {

void set_part_in_status(PartitionInStatus &status,
                        uint32_t partId,
                        uint32_t updateTime,
                        uint64_t writeRate1min,
                        uint64_t writeRate5min,
                        uint64_t readRate1min,
                        uint64_t readRate5min,
                        uint32_t writeRequest1min,
                        uint32_t writeRequest5min,
                        uint32_t readRequest1min,
                        uint32_t readRequest5min,
                        uint32_t resource) {
    status.partId = partId;
    status.updateTime = updateTime;
    status.writeRate1min = writeRate1min;
    status.writeRate5min = writeRate5min;
    status.readRate1min = readRate1min;
    status.readRate5min = readRate5min;
    status.writeRequest1min = writeRequest1min;
    status.writeRequest5min = writeRequest5min;
    status.readRequest1min = readRequest1min;
    status.readRequest5min = readRequest5min;
    status.resource = resource;
}

void set_part_in_metric(protocol::PartitionInMetric *metric,
                        const std::string &topicName,
                        uint32_t partId,
                        uint32_t lastWriteTime,
                        uint32_t lastReadTime,
                        uint64_t writeRate1min,
                        uint64_t writeRate5min,
                        uint64_t readRate1min,
                        uint64_t readRate5min,
                        uint32_t writeRequest1min,
                        uint32_t writeRequest5min,
                        uint32_t readRequest1min,
                        uint32_t readRequest5min,
                        int64_t commitDelay) {
    metric->set_topicname(topicName);
    metric->set_partid(partId);
    metric->set_lastwritetime(lastWriteTime); // s
    metric->set_lastreadtime(lastReadTime);   // s
    metric->set_writerate1min(writeRate1min); // byte
    metric->set_writerate5min(writeRate5min); // byte
    metric->set_readrate1min(readRate1min);   // byte
    metric->set_readrate5min(readRate5min);   // byte
    metric->set_writerequest1min(writeRequest1min);
    metric->set_writerequest5min(writeRequest5min);
    metric->set_readrequest1min(readRequest1min);
    metric->set_readrequest5min(readRequest5min);
    metric->set_commitdelay(commitDelay);
}

void expect_part_in_status(const PartitionInStatus &status,
                           uint32_t partId,
                           uint32_t updateTime,
                           uint64_t writeRate1min,
                           uint64_t writeRate5min,
                           uint64_t readRate1min,
                           uint64_t readRate5min,
                           uint32_t writeRequest1min,
                           uint32_t writeRequest5min,
                           uint32_t readRequest1min,
                           uint32_t readRequest5min,
                           uint32_t resource) {
    ASSERT_EQ(partId, status.partId);
    ASSERT_EQ(updateTime, status.updateTime);
    ASSERT_EQ(writeRate1min, status.writeRate1min);
    ASSERT_EQ(writeRate5min, status.writeRate5min);
    ASSERT_EQ(readRate1min, status.readRate1min);
    ASSERT_EQ(readRate5min, status.readRate5min);
    ASSERT_EQ(writeRequest1min, status.writeRequest1min);
    ASSERT_EQ(writeRequest5min, status.writeRequest5min);
    ASSERT_EQ(readRequest1min, status.readRequest1min);
    ASSERT_EQ(readRequest5min, status.readRequest5min);
    ASSERT_EQ(resource, status.resource);
}

} // namespace admin
} // namespace swift
