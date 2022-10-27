#ifndef ISEARCH_BS_READERCONFIG_H
#define ISEARCH_BS_READERCONFIG_H

#include "build_service/util/Log.h"
#include <autil/legacy/jsonizable.h>
#include "build_service/plugin/ModuleInfo.h"

namespace build_service {
namespace reader {

class ReaderConfig : public autil::legacy::Jsonizable
{
public:
    ReaderConfig() {}
    ~ReaderConfig() {}
public:
    /* override */
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    }
public:

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ReaderConfig);

}
}

#endif //ISEARCH_BS_TOKENIZERCONFIG_H
