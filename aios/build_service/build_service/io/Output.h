#ifndef ISEARCH_BS_OUTPUT_H
#define ISEARCH_BS_OUTPUT_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/config/TaskOutputConfig.h"
#include <autil/legacy/any.h>

namespace build_service {
namespace io {

class Output
{
public:
    Output(const config::TaskOutputConfig& outputConfig)
        : _outputConfig(outputConfig)
    {}
    virtual ~Output() {}
private:
    Output(const Output &);
    Output& operator=(const Output &);
public:
    virtual bool init(const KeyValueMap& params) = 0;
    virtual std::string getType() const = 0;
    virtual bool write(autil::legacy::Any& any) { return false; }
    virtual bool commit() { return false; }

protected:
    void mergeParams(const KeyValueMap& params) {
        for (const auto& kv : params) {
            _outputConfig.addParameters(kv.first, kv.second);
        }
    }

protected:
    config::TaskOutputConfig _outputConfig;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(Output);

}
}

#endif //ISEARCH_BS_OUTPUT_H
