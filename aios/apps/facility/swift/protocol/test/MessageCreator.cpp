#include "swift/protocol/test/MessageCreator.h"

#include <assert.h>
#include <vector>

#include "autil/StringUtil.h"
#include "swift/protocol/Common.pb.h"

namespace swift {
namespace protocol {
AUTIL_LOG_SETUP(swift, MessageCreator);

MessageCreator::MessageCreator() {}

MessageCreator::~MessageCreator() {}

PartitionId MessageCreator::createPartitionId(const std::string &partIdStr) {
    const std::vector<std::string> &items = autil::StringUtil::split(partIdStr, "_", true);
    assert(items.size() > 1);
    std::string topic = items[0];
    uint32_t id = autil::StringUtil::fromString<uint32_t>(items[1]);
    uint64_t masterVersion = items.size() > 2 ? autil::StringUtil::fromString<uint64_t>(items[2]) : 0;
    uint64_t partVersion = items.size() > 3 ? autil::StringUtil::fromString<uint64_t>(items[3]) : 0;
    return createPartitionId(topic, id, masterVersion, partVersion);
}

PartitionId MessageCreator::createPartitionId(const std::string &topicName,
                                              uint32_t id,
                                              uint64_t masterVersion,
                                              uint64_t partVersion) {
    PartitionId partId;
    partId.set_topicname(topicName);
    partId.set_id(id);
    partId.set_from(0);
    partId.set_to(65535);
    partId.set_partitioncount(1);
    partId.set_rangecount(2);
    if (0 != masterVersion && 0 != partVersion) {
        partId.mutable_inlineversion()->set_masterversion(masterVersion);
        partId.mutable_inlineversion()->set_partversion(partVersion);
    }
    return partId;
}

} // namespace protocol
} // namespace swift
