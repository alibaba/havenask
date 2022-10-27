#ifndef ISEARCH_SQL_COMBOCOMPARATOR_H
#define ISEARCH_SQL_COMBOCOMPARATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sql/rank/Comparator.h>

BEGIN_HA3_NAMESPACE(sql);

class ComboComparator
{
public:
    ComboComparator();
    ~ComboComparator();
public:
    virtual bool compare(Row a, Row b) const;
public:
    void addComparator(const Comparator *cmp);
private:
    typedef std::vector<const Comparator *> ComparatorVector;
    ComparatorVector _cmpVector;
};

HA3_TYPEDEF_PTR(ComboComparator);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_SQL_COMBOCOMPARATOR_H
