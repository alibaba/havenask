#ifndef __INDEXLIB_PLUGIN_MODULEINFO_H
#define __INDEXLIB_PLUGIN_MODULEINFO_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/key_value_map.h"
#include <autil/legacy/jsonizable.h>

IE_NAMESPACE_BEGIN(config);

class ModuleInfo : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    bool operator==(const ModuleInfo &other) const;
public:
    std::string moduleName;
    std::string modulePath;
    util::KeyValueMap parameters;
};

typedef std::vector<ModuleInfo> ModuleInfos;
DEFINE_SHARED_PTR(ModuleInfo);

IE_NAMESPACE_END(config);


#endif //__INDEXLIB_PLUGIN_MODULEINFO_H
