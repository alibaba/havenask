#ifndef __SELECT_BUILD_TYPE_PROCESSOR_H
#define __SELECT_BUILD_TYPE_PROCESSOR_H

#include "build_service/util/Log.h"
#include "build_service/processor/DocumentProcessor.h"

namespace build_service {
namespace processor {

class SelectBuildTypeProcessor: public DocumentProcessor
{
public:
    static const std::string PROCESSOR_NAME;
    static const std::string REALTIME_KEY_NAME;
    static const std::string REALTIME_VALUE_NAME;

public:
    SelectBuildTypeProcessor() {
    }
    ~SelectBuildTypeProcessor() {
        //do nothing
    }

    /* override */ bool init(const DocProcessorInitParam &param);
    /* override */ bool process(const document::ExtendDocumentPtr &document);
    /* override */ void batchProcess(const std::vector<document::ExtendDocumentPtr> &docs);
    virtual void destroy();
    SelectBuildTypeProcessor* clone() {
        return new SelectBuildTypeProcessor(*this);
    }
    virtual std::string getDocumentProcessorName() const {
        return PROCESSOR_NAME;
    }
private:
    std::string _notRealtimeKey;
    std::string _notRealtimeValue;
private:
    BS_LOG_DECLARE();
};

}
}

#endif 
