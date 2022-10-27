#ifndef ISEARCH_BS_SWIFTCONFIG_H
#define ISEARCH_BS_SWIFTCONFIG_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/config/SwiftTopicConfig.h"
#include "build_service/proto/BasicDefs.pb.h"
#include <autil/legacy/jsonizable.h>

namespace build_service {
namespace config {

class SwiftConfig : public autil::legacy::Jsonizable
{
public:
    SwiftConfig();
    ~SwiftConfig();
    SwiftConfig(const SwiftConfig &);

public:
    SwiftConfig& operator=(const SwiftConfig &);
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool validate() const;
    const SwiftTopicConfigPtr& getFullBrokerTopicConfig () const;
    const SwiftTopicConfigPtr& getIncBrokerTopicConfig () const;
    bool isFullIncShareBrokerTopic() const {
        return !_fullBrokerTopicConfig && !_incBrokerTopicConfig;
    }
    const std::string& getSwiftReaderConfig(
            proto::BuildStep step) const;
    const std::string& getSwiftWriterConfig(
            proto::BuildStep step) const;
    std::string getSwiftWriterConfig(uint64_t swiftWriterMaxBufferSize,
                                     proto::BuildStep step) const;
    const std::string& getSwiftClientConfig(proto::BuildStep step) const;

private:
    const SwiftTopicConfigPtr& getDefaultTopicConfig() const;

private:
    std::string _readerConfigStr;
    std::string _writerConfigStr;
    std::string _swiftClientConfigStr;
    SwiftTopicConfigPtr _fullBrokerTopicConfig;
    SwiftTopicConfigPtr _incBrokerTopicConfig; 
    mutable SwiftTopicConfigPtr _defaultBrokerTopicConfig;

private:
    static const std::string DEFAULT_SWIFT_WRITER_CONFIG;
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SwiftConfig);

}
}

#endif //ISEARCH_BS_SWIFTCONFIG_H
