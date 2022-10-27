#ifndef ISEARCH_BS_MODULEINFO_H
#define ISEARCH_BS_MODULEINFO_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <autil/legacy/jsonizable.h>
#include <indexlib/config/module_info.h>

namespace build_service {
namespace plugin {

typedef IE_NAMESPACE(config)::ModuleInfo ModuleInfo;

typedef std::vector<IE_NAMESPACE(config)::ModuleInfo> ModuleInfos;
BS_TYPEDEF_PTR(ModuleInfo);

}
}

#endif //ISEARCH_BS_MODULEINFO_H
