#ifndef ISEARCH_BS_CLIOPTION_H
#define ISEARCH_BS_CLIOPTION_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <autil/OptionParser.h>

namespace build_service {
namespace config {

class CLIOption
{
public:
    CLIOption(std::string shortOptName_,
              std::string longOptName_,
              std::string key_,
              bool required_ = false,
              std::string defaultValue_ = std::string())
        : shortOptName(shortOptName_)
        , longOptName(longOptName_)
        , key(key_)
        , required(required_)
        , defaultValue(defaultValue_)
    {
    }
    ~CLIOption() {
    }
public:
    void registerOption(autil::OptionParser &optionParser) const {
        optionParser.addOption(shortOptName, longOptName, key,
                               autil::OptionParser::OPT_STRING, required);
    }
public:
    std::string shortOptName;
    std::string longOptName;
    std::string key;
    bool required;
    std::string defaultValue;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CLIOption);

}
}

#endif //ISEARCH_BS_CLIOPTION_H
