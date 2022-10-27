#ifndef ISEARCH_BS_SWIFTOUTPUT_H
#define ISEARCH_BS_SWIFTOUTPUT_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/io/Output.h"
#include "build_service/io/OutputCreator.h"
#include "build_service/io/IODefine.h"
#include "build_service/config/TaskOutputConfig.h"
#include "build_service/config/SwiftConfig.h"
#include "build_service/util/SwiftClientCreator.h"
#include <swift/common/MessageInfo.h>

namespace build_service {
namespace io {

class SwiftOutput : public Output
{
public:
    SwiftOutput(const config::TaskOutputConfig& outputConfig);
    ~SwiftOutput();

private:
    SwiftOutput(const SwiftOutput&);
    SwiftOutput& operator=(const SwiftOutput &);

public:
    std::string getType() const override { return SWIFT; }

public:
    bool init(const KeyValueMap& params) override;
    bool write(autil::legacy::Any& any) override;
    bool commit() override;
public:
    virtual bool write(SWIFT_NS(common)::MessageInfo& msg);

public:
    int64_t getCommittedCheckpointId() const {
        return _swiftWriter->getCommittedCheckpointId();
    }

private:
    util::SwiftClientCreatorPtr _swiftClientCreator;
    std::unique_ptr<SWIFT_NS(client)::SwiftWriter> _swiftWriter;    

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SwiftOutput);
class SwiftOutputCreator : public OutputCreator
{
public:
    bool init(const config::TaskOutputConfig& outputConfig) override {
        _taskOutputConfig = outputConfig;
        return true;
    }
    
    OutputPtr create(const KeyValueMap& params) override {
        OutputPtr output(new SwiftOutput(_taskOutputConfig));
        if (output->init(params)) {
            return output;
        }
        return OutputPtr();
    }
    
private:
    config::TaskOutputConfig _taskOutputConfig;
};
BS_TYPEDEF_PTR(SwiftOutputCreator);

}
}

#endif
