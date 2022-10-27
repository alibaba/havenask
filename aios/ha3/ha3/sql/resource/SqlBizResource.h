#ifndef ISEARCH_SQL_BIZRESOURCE_H
#define ISEARCH_SQL_BIZRESOURCE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <navi/engine/Resource.h>
#include <suez/turing/common/SessionResource.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <suez/turing/expression/function/FunctionInterfaceCreator.h>
#include <suez/turing/expression/cava/common/CavaPluginManager.h>

BEGIN_HA3_NAMESPACE(sql);

class SqlBizResource : public navi::Resource {
public:
    SqlBizResource(const tensorflow::SessionResourcePtr &_sessionResource);
    ~SqlBizResource();
public:
    std::string getResourceName() const override { return "SqlBizResource"; }
    suez::turing::TableInfoPtr getTableInfo(const std::string &tableName);
    suez::turing::FunctionInterfaceCreatorPtr getFunctionInterfaceCreator();
    suez::turing::CavaPluginManagerPtr getCavaPluginManager();
    resource_reader::ResourceReaderPtr getResourceReader();
    template <typename T>
    T* getObject(const std::string &name) const {
        if (_sessionResource == nullptr) {
            return nullptr;
        }
        T *object = nullptr;
        if (!_sessionResource->sharedObjectMap.get<T>(name, object)) {
            SQL_LOG(WARN, "get resource failed, name [%s]", name.c_str());
            return nullptr;
        }
        return object;
    }

private:
    tensorflow::SessionResourcePtr _sessionResource;
private:
    HA3_LOG_DECLARE();

};

HA3_TYPEDEF_PTR(SqlBizResource);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_SQL_BIZRESOURCE_H
