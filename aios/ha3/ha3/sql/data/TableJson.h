#ifndef ISEARCH_TABLEJSON_H
#define ISEARCH_TABLEJSON_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/legacy/jsonizable.h>

BEGIN_HA3_NAMESPACE(sql);

class TableJson : public autil::legacy::Jsonizable
{
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override {
        json.Jsonize("data", data);
        json.Jsonize("column_name", columnName);
        json.Jsonize("column_type", columnType);
    }
public:
    std::vector<std::vector<autil::legacy::Any>> data;
    std::vector<std::string> columnName;
    std::vector<std::string> columnType;
};

HA3_TYPEDEF_PTR(TableJson);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_TABLEJSON_H
