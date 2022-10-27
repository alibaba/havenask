#ifndef ISEARCH_COLUMN_H
#define ISEARCH_COLUMN_H

#include <ha3/sql/common/common.h>
#include <ha3/sql/data/ColumnData.h>
#include <ha3/sql/data/ColumnSchema.h>
#include <matchdoc/Reference.h>
#include <matchdoc/MatchDoc.h>

BEGIN_HA3_NAMESPACE(sql);

class Column
{
public:
    Column(ColumnSchema *columnSchema, ColumnDataBase *columnData);
    ~Column();
private:
    Column(const Column &);
    Column& operator=(const Column &);
public:
    template<typename T>
    ColumnData<T> *getColumnData() {
        return dynamic_cast<ColumnData<T> *>(_data);
    }

    ColumnSchema *getColumnSchema() const {
        return _schema;
    }

    std::string toString(size_t row) const {
        return _data->toString(row);
    }

    std::string toOriginString(size_t row) const {
        return _data->toOriginString(row);
    }

    inline size_t getRowCount() const {
        return _data->getRowCount();
    }
private:
    ColumnSchema *_schema;
    ColumnDataBase *_data;
};

HA3_TYPEDEF_PTR(Column);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_COLUMN_H
