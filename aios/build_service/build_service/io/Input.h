#ifndef ISEARCH_BS_INPUT_H
#define ISEARCH_BS_INPUT_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/config/TaskInputConfig.h"

namespace build_service {
namespace io {

class Input
{
public:
    enum ErrorCode {
        EC_ERROR,
        EC_OK,
        EC_EOF
    };
public:
    Input(const config::TaskInputConfig& inputConfig)
        : _inputConfig(inputConfig)
    {}
    virtual ~Input() {}
private:
    Input(const Input &);
    Input& operator=(const Input &);
    
public:
    virtual bool init(const KeyValueMap& params) = 0;
    virtual std::string getType() const = 0;
    virtual ErrorCode read(autil::legacy::Any& any) = 0;

protected:
    config::TaskInputConfig _inputConfig;
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(Input);

}
}

#endif //ISEARCH_BS_INPUT_H
