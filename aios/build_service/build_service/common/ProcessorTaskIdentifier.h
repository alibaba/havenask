#ifndef ISEARCH_BS_PROCESSORTASKIDENTIFIER_H
#define ISEARCH_BS_PROCESSORTASKIDENTIFIER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/common/TaskIdentifier.h"

namespace build_service {
namespace common {

class ProcessorTaskIdentifier : public TaskIdentifier
{
public:
    ProcessorTaskIdentifier();
    ~ProcessorTaskIdentifier();

private:
    ProcessorTaskIdentifier(const ProcessorTaskIdentifier &);
    ProcessorTaskIdentifier& operator=(const ProcessorTaskIdentifier &);
    
public:
    bool fromString(const std::string &content) override;
    std::string toString() const override;

    void setDataDescriptionId(uint32_t id) { _dataDescriptionId = id; }
    uint32_t getDataDescriptionId() const { return _dataDescriptionId; }

private:
    uint32_t _dataDescriptionId;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ProcessorTaskIdentifier);

}
}

#endif //ISEARCH_BS_PROCESSORTASKIDENTIFIER_H
