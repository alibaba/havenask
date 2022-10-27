#ifndef ISEARCH_COLUMNCOMPARATOR_H
#define ISEARCH_COLUMNCOMPARATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sql/rank/Comparator.h>
#include <ha3/sql/data/ColumnData.h>

BEGIN_HA3_NAMESPACE(sql);

template <typename T>
class ColumnAscComparator : public Comparator
{
public:
    ColumnAscComparator(const ColumnData<T> *columnData)
        : _columnData(columnData)
    {}
    ~ColumnAscComparator() {}
private:
    ColumnAscComparator(const ColumnAscComparator &);
    ColumnAscComparator& operator=(const ColumnAscComparator &);
public:
    bool compare(Row a, Row b) const override {
        return _columnData->get(a) < _columnData->get(b);
    }
private:
    const ColumnData<T> *_columnData;
};


template <typename T>
class ColumnDescComparator : public Comparator
{
public:
    ColumnDescComparator(const ColumnData<T> *columnData)
        : _columnData(columnData)
    {}
    ~ColumnDescComparator() {}
private:
    ColumnDescComparator(const ColumnDescComparator &);
    ColumnDescComparator& operator=(const ColumnDescComparator &);
public:
    bool compare(Row a, Row b) const override {
        return _columnData->get(b) < _columnData->get(a);
    }
private:
    const ColumnData<T> *_columnData;
};

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_COLUMNCOMPARATOR_H
