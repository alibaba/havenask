#ifndef ISEARCH_BS_INPUTCREATOR_H
#define ISEARCH_BS_INPUTCREATOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/io/Input.h"

namespace build_service {
namespace io {

class InputCreator
{
public:
    InputCreator() {}
    virtual ~InputCreator() {}
private:
    InputCreator(const InputCreator &);
    InputCreator& operator=(const InputCreator &);
    
public:
    virtual bool init(const config::TaskInputConfig& inputConfig) = 0;
    virtual InputPtr create(const KeyValueMap& params) = 0;
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(InputCreator);

}
}

#endif //ISEARCH_BS_INPUTCREATOR_H
