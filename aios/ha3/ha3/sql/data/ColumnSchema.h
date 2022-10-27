#ifndef ISEARCH_COLUMNSCHEMA_H
#define ISEARCH_COLUMNSCHEMA_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sql/data/DataCommon.h>

BEGIN_HA3_NAMESPACE(sql);

class ColumnSchema
{
public:
    ColumnSchema(std::string name, ValueType type);
    ~ColumnSchema();
private:
    ColumnSchema(const ColumnSchema &);
    ColumnSchema& operator=(const ColumnSchema &);
public:
    const std::string &getName() const {
        return _name;
    }
    ValueType getType() const {
        return _type;
    }
    bool operator == (const ColumnSchema &other) const {
        return _name == other._name && _type == other._type;
    }
    bool operator != (const ColumnSchema &other) const {
        return !(*this == other);
    }
    std::string toString() const;

private:
    std::string _name;
    ValueType _type;
};

HA3_TYPEDEF_PTR(ColumnSchema);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_COLUMNSCHEMA_H
