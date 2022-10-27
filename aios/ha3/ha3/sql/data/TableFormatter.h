#ifndef ISEARCH_TABLEFORMATTER_H
#define ISEARCH_TABLEFORMATTER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sql/data/Table.h>

BEGIN_HA3_NAMESPACE(sql);

class TableFormatter
{
public:
    TableFormatter();
    ~TableFormatter();
private:
    TableFormatter(const TableFormatter &);
    TableFormatter& operator=(const TableFormatter &);
public:
    static std::string format(TablePtr &table, const std::string &type);
};

HA3_TYPEDEF_PTR(TableFormatter);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_TABLEFORMATTER_H
