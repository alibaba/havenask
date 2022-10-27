#include <ha3/common/GlobalVariableManager.h>

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, GlobalVariableManager);

GlobalVariableManager::GlobalVariableManager()
{
}

GlobalVariableManager::~GlobalVariableManager() {
}

AttributeItemMapPtr GlobalVariableManager::getNeedSerializeGlobalVariables() const {
    AttributeItemMapPtr ret(new AttributeItemMap);
    for (GlobalVariableMap::const_iterator it = _globalVariableMap.begin();
         it != _globalVariableMap.end(); ++it)
    {
        if (it->second.first) {
            (*ret)[it->first] = it->second.second;
        }
    }
    return ret;
}

END_HA3_NAMESPACE(common);

