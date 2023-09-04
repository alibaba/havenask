#pragma once

#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "swift/common/Common.h"
#include "swift/protocol/Common.pb.h"

namespace swift {
namespace protocol {

class MessageCreator {
public:
    MessageCreator();
    ~MessageCreator();

private:
    MessageCreator(const MessageCreator &);
    MessageCreator &operator=(const MessageCreator &);

public:
    static PartitionId createPartitionId(const std::string &partIdStr);
    static PartitionId
    createPartitionId(const std::string &topicName, uint32_t id, uint64_t masterVersion = 0, uint64_t partVersion = 0);

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(MessageCreator);

} // namespace protocol
} // namespace swift
