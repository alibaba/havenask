#ifndef ISEARCH_BS_TRADITIONALTABLE_H
#define ISEARCH_BS_TRADITIONALTABLE_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <autil/legacy/jsonizable.h>

namespace build_service {
namespace analyzer {

class TraditionalTable : public autil::legacy::Jsonizable
{
public:
    TraditionalTable() {}
    ~TraditionalTable() {}
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
private:
    static bool hexToUint16(const std::string &str, uint16_t &value);
public:
    std::string tableName;
    std::map<uint16_t, uint16_t> table;
};

class TraditionalTables : public autil::legacy::Jsonizable
{
public:
    TraditionalTables();
    ~TraditionalTables();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
public:
    const TraditionalTable *getTraditionalTable(const std::string &tableName) const;
    void addTraditionalTable(const TraditionalTable &table);
private:
    std::vector<TraditionalTable> _tables;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TraditionalTables);

}
}

#endif //ISEARCH_BS_TRADITIONALTABLE_H
