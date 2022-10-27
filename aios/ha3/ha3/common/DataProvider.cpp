#include <ha3/common/DataProvider.h>

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, DataProvider);

DataProvider::DataProvider(const DataProvider &other) {
    this->_globalVariableMgr = other._globalVariableMgr;
    this->_isSubScope = other._isSubScope;
}

DataProvider& DataProvider::operator=(const DataProvider &other) {
    this->_globalVariableMgr = other._globalVariableMgr;
    this->_isSubScope = other._isSubScope;
    return *this;
}

DataProvider::DataProvider()
    : _isSubScope(false)
{
    _globalVariableMgr.reset(new GlobalVariableManager());
}

DataProvider::DataProvider(const GlobalVariableManagerPtr &globalVariableManagerPtr)
    : _globalVariableMgr(globalVariableManagerPtr)
    , _isSubScope(false)
{
}

DataProvider::~DataProvider() {
}

AttributeItemMapPtr DataProvider::getNeedSerializeGlobalVariables() const {
    return _globalVariableMgr->getNeedSerializeGlobalVariables();
}

END_HA3_NAMESPACE(common);
