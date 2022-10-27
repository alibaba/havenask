#ifndef ISEARCH_BS_OUTPUTCREATOR_H
#define ISEARCH_BS_OUTPUTCREATOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/io/Output.h"

namespace build_service {
namespace io {

class OutputCreator
{
public:
    OutputCreator() {}
    virtual ~OutputCreator() {}
private:
    OutputCreator(const OutputCreator &);
    OutputCreator& operator=(const OutputCreator &);
public:
    virtual bool init(const config::TaskOutputConfig& outputConfig) = 0;
    virtual OutputPtr create(const KeyValueMap& params) = 0;
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(OutputCreator);

}
}

#endif //ISEARCH_BS_OUTPUTCREATOR_H
