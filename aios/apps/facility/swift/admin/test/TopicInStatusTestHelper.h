#pragma once

#include <stdint.h>
#include <string>

namespace swift {
namespace protocol {
class PartitionInMetric;
} // namespace protocol

namespace admin {
struct PartitionInStatus;

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
                        uint32_t resource = 10);

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
                        int64_t dfsTimeout);

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
                           uint32_t resource = 10);

} // namespace admin
} // namespace swift
