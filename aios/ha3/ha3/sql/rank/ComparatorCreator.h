#ifndef ISEARCH_COMPARATORCREATOR_H
#define ISEARCH_COMPARATORCREATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sql/data/Table.h>
#include <ha3/sql/rank/ComboComparator.h>
#include <autil/mem_pool/Pool.h>

BEGIN_HA3_NAMESPACE(sql);

class ComparatorCreator
{
public:
    ComparatorCreator() {}
    ~ComparatorCreator() {}
public:
    static ComboComparatorPtr createOneDimColumnComparator(const TablePtr &table,
            const std::string &refName, const bool order);
    static ComboComparatorPtr createComboComparator(const TablePtr &table,
            const std::vector<std::string> &refNames,
            const std::vector<bool> &orders,
            autil::mem_pool::Pool *pool);
private:
    ComparatorCreator(const ComparatorCreator &);
    ComparatorCreator& operator=(const ComparatorCreator &);
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(sql);

#endif // ISEARCH_COMPARATORCREATOR_H
