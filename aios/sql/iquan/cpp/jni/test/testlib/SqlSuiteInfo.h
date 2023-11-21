#pragma once

#include <string>
#include <vector>

#include "autil/legacy/any.h"
#include "autil/legacy/jsonizable.h"
#include "iquan/common/Common.h"
#include "iquan/jni/IquanDqlResponse.h"

namespace iquan {

class DefaultCatalogPath : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("default_catalog_name", catalogName);
        json.Jsonize("default_database_name", databaseName);
    }
    bool isValid() {
        return !catalogName.empty() && !databaseName.empty();
    }

public:
    std::string catalogName;
    std::string databaseName;
};

class DatabaseInfo : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("database_name", databaseName);
        json.Jsonize("functions", functions, functions);
    }

public:
    std::string databaseName;
    std::vector<std::string> functions;
};

class DbCatalogInfo : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("catalog_name", catalogName);
        json.Jsonize("databases", databaseInfos);
    }

public:
    std::string catalogName;
    std::vector<DatabaseInfo> databaseInfos;
};

class SqlQueryInfo : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("sql", sql, sql);
        json.Jsonize("sqls", sqls, {});
        json.Jsonize("sql_params", sqlParams, {});
        json.Jsonize("dynamic_params", dynamicParams, {});
    }

    bool isValid() {
        if (sql.empty() && sqls.empty()) {
            return false;
        }
        if (!sql.empty()) {
            sqls.clear();
            sqls.push_back(sql);
        }
        if (!dynamicParams.empty() && sqls.size() != dynamicParams.size()) {
            return false;
        }
        return true;
    }

public:
    std::string sql;
    std::vector<std::string> sqls;
    autil::legacy::json::JsonMap sqlParams;
    std::vector<std::vector<autil::legacy::Any>> dynamicParams;
};

class SqlQueryInfos : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("querys", querys, {});
    }

    bool isValid() {
        if (querys.empty()) {
            return false;
        }
        for (SqlQueryInfo &query : querys) {
            if (!query.isValid()) {
                return false;
            }
        }
        return true;
    }

public:
    std::vector<SqlQueryInfo> querys;
};

class SqlPlanInfo : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("plan", plan);
    }

public:
    SqlPlan plan;
    std::shared_ptr<autil::legacy::RapidDocument> documentPtr;
};

} // namespace iquan
