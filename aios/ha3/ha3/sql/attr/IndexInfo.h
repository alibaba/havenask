#ifndef ISEARCH_INDEXINFO_H
#define ISEARCH_INDEXINFO_H

#include <ha3/sql/common/common.h>
#include <autil/legacy/jsonizable.h>

BEGIN_HA3_NAMESPACE(sql);

class IndexInfo : public autil::legacy::Jsonizable {
public:
    IndexInfo() {
    }
    IndexInfo(std::string name_, std::string type_)
        : name(name_)
        , type(type_)
    {
    }
    ~IndexInfo() {
    }
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("name", name);
        json.Jsonize("type", type);
    }
public:
    std::string name;
    std::string type;
};

typedef std::map<std::string, sql::IndexInfo> IndexInfoMapType;

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_INDEXINFO_H
