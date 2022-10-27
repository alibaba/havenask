#ifndef ISEARCH_TABLESCHEMA_H
#define ISEARCH_TABLESCHEMA_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sql/data/ColumnSchema.h>

BEGIN_HA3_NAMESPACE(sql);

class TableSchema
{
public:
    TableSchema() {}
    ~TableSchema() {}
private:
    TableSchema(const TableSchema &);
    TableSchema& operator=(const TableSchema &);
public:
    bool addColumnSchema(const ColumnSchemaPtr &columnSchema);
    // bool addColumnSchema(const std::string &name, ValueType vt);
    bool eraseColumnSchema(const std::string &name);
    ColumnSchema* getColumnSchema(const std::string &name) const;
    std::string toString() const;
    bool operator == (const TableSchema& other) const;
private:
    std::map<std::string, ColumnSchemaPtr> _columnSchema;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(TableSchema);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_TABLESCHEMA_H
