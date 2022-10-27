#ifndef ISEARCH_BS_SWIFTTOPICCONFIG_H
#define ISEARCH_BS_SWIFTTOPICCONFIG_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "build_service/proto/JsonizableProtobuf.h"
#include <autil/legacy/jsonizable.h>

namespace build_service {
namespace config {

class SwiftTopicConfig : public proto::JsonizableProtobuf<swift::protocol::TopicCreationRequest>
{
public:
    SwiftTopicConfig();
    ~SwiftTopicConfig();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool validate(bool allowMemoryPrefer = false) const;
	std::string getTopicMode() const; 
	uint32_t getPartitionCount() const;
    bool hasPartitionRestrict() const;

private:
    static std::string topicModeToStr(swift::protocol::TopicMode topicMode);
    static swift::protocol::TopicMode strTotopicMode(const std::string &str);
private:
    static const int64_t DEFAULT_NO_MORE_MSG_PERIOD;
    static const int64_t DEFAULT_MAX_COMMIT_INTERVAL;
    static const double DEFAULT_BULDER_MEM_TO_PROCESSOR_BUFFER_FACTOR;
public:
    static const std::string TOPIC_MODE_SECURITY_STR;
    static const std::string TOPIC_MODE_MEMORY_ONLY_STR;
    static const std::string TOPIC_MODE_MEMORY_PREFER_STR;
    static const std::string TOPIC_MODE_NORMAL_STR;
public:
    // only for tuning memory prefer topic config,
    // do not intend to open for common users.
    int64_t noMoreMsgPeriod;
    int64_t maxCommitInterval;
    std::map<std::string, uint64_t> writerMaxBufferSize;
    double builderMemToProcessorBufferFactor;
    bool enablePartitionRestrict;
    std::string readerConfigStr;
    std::string writerConfigStr;
    std::string swiftClientConfigStr;
    std::string swiftClientShareMode;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SwiftTopicConfig);
}
}

#endif //ISEARCH_BS_SWIFTTOPICCONFIG_H
