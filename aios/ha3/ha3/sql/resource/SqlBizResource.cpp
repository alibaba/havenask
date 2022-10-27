#include <ha3/sql/resource/SqlBizResource.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;
BEGIN_HA3_NAMESPACE(sql);

HA3_LOG_SETUP(sql, SqlBizResource);

SqlBizResource::SqlBizResource(const SessionResourcePtr &sessionResource)
    : _sessionResource(sessionResource)
{
}

SqlBizResource::~SqlBizResource() {}

TableInfoPtr SqlBizResource::getTableInfo(const std::string &tableName) {

    auto tableInfo = _sessionResource->dependencyTableInfoMap.find(tableName);
    if (tableInfo != _sessionResource->dependencyTableInfoMap.end()) {
        return tableInfo->second;
    }
    SQL_LOG(WARN, "get table info failed, tableName [%s]", tableName.c_str());
    return TableInfoPtr();
}

FunctionInterfaceCreatorPtr SqlBizResource::getFunctionInterfaceCreator() {
    return  _sessionResource->functionInterfaceCreator;
}

CavaPluginManagerPtr SqlBizResource::getCavaPluginManager() {
    return _sessionResource->cavaPluginManager;
}

resource_reader::ResourceReaderPtr SqlBizResource::getResourceReader() {
    return _sessionResource->resourceReader;
}

END_HA3_NAMESPACE(sql);
