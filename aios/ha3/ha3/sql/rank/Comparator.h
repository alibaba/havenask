#ifndef ISEARCH_SQL_COMPARATOR_H
#define ISEARCH_SQL_COMPARATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sql/data/Row.h>

BEGIN_HA3_NAMESPACE(sql);

class Comparator
{
public:
    Comparator() {}
    virtual ~Comparator() {}
public:
    virtual bool compare(Row a, Row b) const  = 0;
};

HA3_TYPEDEF_PTR(Comparator);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_SQL_COMPARATOR_H
