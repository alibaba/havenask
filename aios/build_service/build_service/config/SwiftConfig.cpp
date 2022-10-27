#include "build_service/config/SwiftConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/proto/ProtoUtil.h"
#include <autil/legacy/json.h>
#include <autil/StringUtil.h>
#include <regex>

using namespace std;

using namespace build_service::proto;
namespace build_service {
namespace config {
BS_LOG_SETUP(config, SwiftConfig);

const string SwiftConfig::DEFAULT_SWIFT_WRITER_CONFIG = "functionChain=hashId2partId;mode=async|safe";

SwiftConfig::SwiftConfig()
    :_readerConfigStr("")
    ,_writerConfigStr(DEFAULT_SWIFT_WRITER_CONFIG)
{
}

SwiftConfig::~SwiftConfig() {
}

SwiftConfig::SwiftConfig(const SwiftConfig &other)
    : _readerConfigStr(other._readerConfigStr)
    , _writerConfigStr(other._writerConfigStr)
    , _swiftClientConfigStr(other._swiftClientConfigStr)
{
    if (other._fullBrokerTopicConfig) {
        _fullBrokerTopicConfig.reset(new SwiftTopicConfig(*other._fullBrokerTopicConfig));        
    }
    if (other._incBrokerTopicConfig) {
        _incBrokerTopicConfig.reset(new SwiftTopicConfig(*other._incBrokerTopicConfig));
    }
    if (other._defaultBrokerTopicConfig) {
        _defaultBrokerTopicConfig.reset(new SwiftTopicConfig(*other._defaultBrokerTopicConfig));
    }    
}

SwiftConfig& SwiftConfig::operator=(const SwiftConfig &other)
{
    _readerConfigStr = other._readerConfigStr;
    _writerConfigStr = other._writerConfigStr;
    _swiftClientConfigStr = other._swiftClientConfigStr;
    if (other._fullBrokerTopicConfig) {
        _fullBrokerTopicConfig.reset(new SwiftTopicConfig(*other._fullBrokerTopicConfig));        
    }
    if (other._incBrokerTopicConfig) {
        _incBrokerTopicConfig.reset(new SwiftTopicConfig(*other._incBrokerTopicConfig));
    }
    if (other._defaultBrokerTopicConfig) {
        _defaultBrokerTopicConfig.reset(new SwiftTopicConfig(*other._defaultBrokerTopicConfig));
    }
    return *this;
}
    
void SwiftConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize(SWIFT_READER_CONFIG, _readerConfigStr, _readerConfigStr);
    json.Jsonize(SWIFT_WRITER_CONFIG, _writerConfigStr, _writerConfigStr);    
    json.Jsonize(SWIFT_CLIENT_CONFIG, _swiftClientConfigStr, _swiftClientConfigStr);
    if (json.GetMode() == FROM_JSON) {
        autil::legacy::json::JsonMap jsonMap = json.GetMap();
        auto iter = jsonMap.find(DEFAULT_SWIFT_BROKER_TOPIC_CONFIG);
        if (iter != jsonMap.end()) {
            _defaultBrokerTopicConfig.reset(new SwiftTopicConfig());
            json.Jsonize(DEFAULT_SWIFT_BROKER_TOPIC_CONFIG, *_defaultBrokerTopicConfig.get());
        } 
        iter = jsonMap.find(FULL_SWIFT_BROKER_TOPIC_CONFIG);
        if (iter != jsonMap.end()) {
            _fullBrokerTopicConfig.reset(new SwiftTopicConfig());
            json.Jsonize(FULL_SWIFT_BROKER_TOPIC_CONFIG, *_fullBrokerTopicConfig.get());
        } 
        iter = jsonMap.find(INC_SWIFT_BROKER_TOPIC_CONFIG);
        if (iter != jsonMap.end()) {
            _incBrokerTopicConfig.reset(new SwiftTopicConfig());
            json.Jsonize(INC_SWIFT_BROKER_TOPIC_CONFIG, *_incBrokerTopicConfig.get());
        }
    } else {
        if (_defaultBrokerTopicConfig) {
            json.Jsonize(DEFAULT_SWIFT_BROKER_TOPIC_CONFIG, *_defaultBrokerTopicConfig.get());
        }
        if (_fullBrokerTopicConfig) {
            json.Jsonize(FULL_SWIFT_BROKER_TOPIC_CONFIG, *_fullBrokerTopicConfig.get()); 
        }
        if (_incBrokerTopicConfig) {
            json.Jsonize(INC_SWIFT_BROKER_TOPIC_CONFIG, *_incBrokerTopicConfig.get()); 
        }
    }
}

bool SwiftConfig::validate() const {
    if (_defaultBrokerTopicConfig) {
        if (_fullBrokerTopicConfig || _incBrokerTopicConfig) {
            BS_LOG(ERROR, "once specify [%s] config, cannot specify other topic configs",
                   DEFAULT_SWIFT_BROKER_TOPIC_CONFIG.c_str());
            return false;
        }
        return _defaultBrokerTopicConfig->validate();
    }

    if (!_fullBrokerTopicConfig && !_incBrokerTopicConfig) {
        // specify no topic config, use default.
        return true;
    }
        
    if (!_fullBrokerTopicConfig || !_incBrokerTopicConfig) {
        BS_LOG(ERROR, "[%s] and [%s] should be specified in pairs.",
               FULL_SWIFT_BROKER_TOPIC_CONFIG.c_str(),
               INC_SWIFT_BROKER_TOPIC_CONFIG.c_str());
        return false;
    }
    return _fullBrokerTopicConfig->validate(true) && _incBrokerTopicConfig->validate();
}


const SwiftTopicConfigPtr& SwiftConfig::getFullBrokerTopicConfig() const {
    return _fullBrokerTopicConfig ? _fullBrokerTopicConfig : getDefaultTopicConfig();
}

const SwiftTopicConfigPtr& SwiftConfig::getIncBrokerTopicConfig() const {
    return _incBrokerTopicConfig ? _incBrokerTopicConfig : getDefaultTopicConfig();
}

const SwiftTopicConfigPtr& SwiftConfig::getDefaultTopicConfig() const {
    if (!_defaultBrokerTopicConfig) {
        _defaultBrokerTopicConfig.reset(new SwiftTopicConfig());
    }
    return _defaultBrokerTopicConfig;
}

string SwiftConfig::getSwiftWriterConfig(uint64_t swiftWriterMaxBufferSize,
                                         proto::BuildStep step) const
{
    regex pattern(R"(maxBufferSize=\d+)");
    std::smatch m;
    string replaceStr = "maxBufferSize=" +
                        autil::StringUtil::toString(swiftWriterMaxBufferSize);
    const std::string& writerConfigStr = getSwiftWriterConfig(step);
    if (regex_search(writerConfigStr, m, pattern)) {
        return regex_replace(writerConfigStr, pattern, replaceStr);
    } else {
        return replaceStr + ";" + writerConfigStr;
    }
}

const std::string& SwiftConfig::getSwiftReaderConfig(proto::BuildStep step) const {
    if (step == proto::BUILD_STEP_FULL) {
        if (_fullBrokerTopicConfig && !_fullBrokerTopicConfig->readerConfigStr.empty()) {
            return _fullBrokerTopicConfig->readerConfigStr;
        }
    } else if (step == proto::BUILD_STEP_INC) {
        if (_incBrokerTopicConfig && !_incBrokerTopicConfig->readerConfigStr.empty()) {
            return _incBrokerTopicConfig->readerConfigStr;
        }
    }
    return _readerConfigStr;
}

const std::string& SwiftConfig::getSwiftWriterConfig(proto::BuildStep step) const {
    if (step == proto::BUILD_STEP_FULL) {
        if (_fullBrokerTopicConfig && !_fullBrokerTopicConfig->writerConfigStr.empty()) {
            return _fullBrokerTopicConfig->writerConfigStr;
        }
    } else if (step == proto::BUILD_STEP_INC) {
        if (_incBrokerTopicConfig && !_incBrokerTopicConfig->writerConfigStr.empty()) {
            return _incBrokerTopicConfig->writerConfigStr;
        }
    }
    return _writerConfigStr;
}

const std::string& SwiftConfig::getSwiftClientConfig(proto::BuildStep step) const {
    if (step == proto::BUILD_STEP_FULL) {
        if (_fullBrokerTopicConfig && !_fullBrokerTopicConfig->swiftClientConfigStr.empty()) {
            return _fullBrokerTopicConfig->swiftClientConfigStr;
        }
    } else if (step == proto::BUILD_STEP_INC) {
        if (_incBrokerTopicConfig && !_incBrokerTopicConfig->swiftClientConfigStr.empty()) {
            return _incBrokerTopicConfig->swiftClientConfigStr;
        }
    }
    return _swiftClientConfigStr;
    
}


}
}
