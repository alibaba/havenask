#include "build_service/config/SwiftTopicConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include <http_arpc/ProtoJsonizer.h>

using namespace std;
using namespace swift::protocol;

namespace build_service {
namespace config {
BS_LOG_SETUP(config, SwiftTopicConfig);
    
const std::string SwiftTopicConfig::TOPIC_MODE_SECURITY_STR = "security";
const std::string SwiftTopicConfig::TOPIC_MODE_MEMORY_ONLY_STR = "memory_only";
const std::string SwiftTopicConfig::TOPIC_MODE_MEMORY_PREFER_STR = "memory_prefer";
const std::string SwiftTopicConfig::TOPIC_MODE_NORMAL_STR = "normal";
const int64_t SwiftTopicConfig::DEFAULT_NO_MORE_MSG_PERIOD = 1 * 60 * 1000000; // 1 mininuts
const int64_t SwiftTopicConfig::DEFAULT_MAX_COMMIT_INTERVAL = 5 * 60 * 1000000; // 5 mininuts
const double SwiftTopicConfig::DEFAULT_BULDER_MEM_TO_PROCESSOR_BUFFER_FACTOR = 1.5;

SwiftTopicConfig::SwiftTopicConfig()
    : noMoreMsgPeriod(DEFAULT_NO_MORE_MSG_PERIOD)
    , maxCommitInterval(DEFAULT_MAX_COMMIT_INTERVAL)
    , builderMemToProcessorBufferFactor(DEFAULT_BULDER_MEM_TO_PROCESSOR_BUFFER_FACTOR)
    , enablePartitionRestrict(true)
{
    auto &topicConfig = get();
    topicConfig.set_deletetopicdata(false);
    topicConfig.set_compressmsg(false);
    topicConfig.set_resource(1);
    topicConfig.set_partitioncount(1);
}

SwiftTopicConfig::~SwiftTopicConfig() {
}

#define JSONIZE_LEGACY_FIELD(json_field_name, proto_field_name)         \
    do {                                                                \
        decltype(get().proto_field_name()) _fieldvalue;                  \
        json.Jsonize(#json_field_name, _fieldvalue, get().proto_field_name()); \
        get().set_##proto_field_name(_fieldvalue);                      \
    } while (0)

void SwiftTopicConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    // jsonize proto
    proto::JsonizableProtobuf<swift::protocol::TopicCreationRequest>::Jsonize(json);

    // Due to legacy code problem (json mode is set to FROM_JSON in base class)
    // these fields will not be serialized
    json.Jsonize("no_more_msg_period", noMoreMsgPeriod, noMoreMsgPeriod);
    json.Jsonize("max_commit_interval", maxCommitInterval, maxCommitInterval);
    json.Jsonize("writer_max_buffer_size", writerMaxBufferSize, writerMaxBufferSize);
    json.Jsonize("builder_mem_to_processor_buffer_factor",
                 builderMemToProcessorBufferFactor, builderMemToProcessorBufferFactor);
    json.Jsonize("enable_partition_restrict",
                 enablePartitionRestrict, enablePartitionRestrict);
    // jsonize leagcy json field
    if (autil::legacy::Jsonizable::FROM_JSON == json.GetMode()) {
        JSONIZE_LEGACY_FIELD(partition_count, partitioncount);
        JSONIZE_LEGACY_FIELD(partition_max_buffer_size, partitionmaxbuffersize);
        JSONIZE_LEGACY_FIELD(partition_min_buffer_size, partitionminbuffersize);
        JSONIZE_LEGACY_FIELD(partition_resource, resource);
        JSONIZE_LEGACY_FIELD(partition_limit, partitionlimit);
        JSONIZE_LEGACY_FIELD(partition_file_buffer_size, partitionfilebuffersize);
        JSONIZE_LEGACY_FIELD(obsolete_file_interval, obsoletefiletimeinterval);
        JSONIZE_LEGACY_FIELD(reserved_file_count, reservedfilecount);
        JSONIZE_LEGACY_FIELD(delete_topic_data, deletetopicdata);
        JSONIZE_LEGACY_FIELD(need_field_filter, needfieldfilter);
        JSONIZE_LEGACY_FIELD(compress_msg, compressmsg);
        string topicModeStr;
        json.Jsonize("topic_mode", topicModeStr, topicModeStr);
        if (!topicModeStr.empty()) {
            get().set_topicmode(strTotopicMode(topicModeStr));
        }
        string topicGroup;
        json.Jsonize("topic_group", topicGroup, topicGroup);
        if (!topicGroup.empty()) {
            get().set_topicgroup(topicGroup);
        }
        json.Jsonize("reader_config", readerConfigStr, readerConfigStr);
        json.Jsonize("writer_config", writerConfigStr, writerConfigStr);
        json.Jsonize(SWIFT_CLIENT_CONFIG, swiftClientConfigStr, swiftClientConfigStr);
    } else {
        if (!readerConfigStr.empty()) {
            json.Jsonize("reader_config", readerConfigStr, readerConfigStr);
        }
        if (!writerConfigStr.empty()) {
            json.Jsonize("writer_config", writerConfigStr, writerConfigStr);
        }
        if (!swiftClientConfigStr.empty()) {
            json.Jsonize(SWIFT_CLIENT_CONFIG, swiftClientConfigStr, swiftClientConfigStr);
        }
    }
        
}

bool SwiftTopicConfig::validate(bool allowMemoryPrefer) const {
    if (get().has_topicmode()) {
        auto topicMode = get().topicmode();
        if (topicMode == TOPIC_MODE_MEMORY_ONLY) {
            BS_LOG(ERROR, "un-supported topic mode [%s]", topicModeToStr(topicMode).c_str());
            return false;
        }

        if (topicMode == TOPIC_MODE_MEMORY_PREFER && !allowMemoryPrefer) {
            BS_LOG(ERROR, "only full broker topic can be configured as [%s]",
                   topicModeToStr(topicMode).c_str());
            return false;
        }
    }
    if (noMoreMsgPeriod <= 0)
    {
        BS_LOG(ERROR, "no_more_msg_period[%ld] should be a positive integer", noMoreMsgPeriod);
        return false;
    }

    if (maxCommitInterval <= 0)
    {
        BS_LOG(ERROR, "max_commit_interval[%ld] should be a positive integer", maxCommitInterval);
        return false;
    }

    if (get().partitioncount() <= 0) {
        BS_LOG(ERROR, "partition_count[%d] should be a positive integer", get().partitioncount());
        return false;
    }
    return true;
}

string SwiftTopicConfig::topicModeToStr(TopicMode topicMode) {
    switch (topicMode) {
    case TOPIC_MODE_SECURITY:
        return TOPIC_MODE_SECURITY_STR;
    case TOPIC_MODE_MEMORY_ONLY:
        return TOPIC_MODE_MEMORY_ONLY_STR;
    case TOPIC_MODE_MEMORY_PREFER:
        return TOPIC_MODE_MEMORY_PREFER_STR;
    default:
        return TOPIC_MODE_NORMAL_STR;
    }
}

TopicMode SwiftTopicConfig::strTotopicMode(const std::string &str) {
    if (TOPIC_MODE_SECURITY_STR == str) {
        return TOPIC_MODE_SECURITY;
    } else if (TOPIC_MODE_MEMORY_ONLY_STR == str) {
        return TOPIC_MODE_MEMORY_ONLY;
    } else if (TOPIC_MODE_MEMORY_PREFER_STR == str) {
        return TOPIC_MODE_MEMORY_PREFER;
    } else {
        return TOPIC_MODE_NORMAL;
    }
}

std::string SwiftTopicConfig::getTopicMode() const {
	if (get().has_topicmode()) {
		return topicModeToStr(get().topicmode());
	}
	return "";
}

uint32_t SwiftTopicConfig::getPartitionCount() const {
	return get().partitioncount();
}

bool SwiftTopicConfig::hasPartitionRestrict() const {
    if (get().has_topicmode()) {
        return get().topicmode() == TOPIC_MODE_MEMORY_PREFER && enablePartitionRestrict;
    }
    return false;
}


}
}
