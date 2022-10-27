#ifndef ISEARCH_ONEDIMCOLUMNCOMPARATOR_H
#define ISEARCH_ONEDIMCOLUMNCOMPARATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sql/rank/ComboComparator.h>

BEGIN_HA3_NAMESPACE(sql);

template <typename T>
class OneDimColumnAscComparator : public ComboComparator
{
public:
    OneDimColumnAscComparator(const ColumnData<T> *columnData)
        : _columnData(columnData)
    {}
    ~OneDimColumnAscComparator() {}
private:
    OneDimColumnAscComparator(const OneDimColumnAscComparator &);
    OneDimColumnAscComparator& operator=(const OneDimColumnAscComparator &);
public:
    bool compare(Row a, Row b) const override {
        return _columnData->get(a) < _columnData->get(b);
    }
private:
    const ColumnData<T> *_columnData;
};


template <typename T>
class OneDimColumnDescComparator : public ComboComparator
{
public:
    OneDimColumnDescComparator(const ColumnData<T> *columnData)
        : _columnData(columnData)
    {}
    ~OneDimColumnDescComparator() {}
private:
    OneDimColumnDescComparator(const OneDimColumnDescComparator &);
    OneDimColumnDescComparator& operator=(const OneDimColumnDescComparator &);
public:
    bool compare(Row a, Row b) const override {
        return _columnData->get(b) < _columnData->get(a);
    }
private:
    const ColumnData<T> *_columnData;
};

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_ONEDIMCOLUMNCOMPARATOR_H
