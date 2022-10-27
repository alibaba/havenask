#ifndef ISEARCH_COLUMN_DATA_H
#define ISEARCH_COLUMN_DATA_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sql/data/DataCommon.h>
#include <ha3/sql/data/Row.h>
#include <matchdoc/Reference.h>
#include <autil/Hash64Function.h>

BEGIN_HA3_NAMESPACE(sql);

class Column;

class ColumnDataBase {
public:
    ColumnDataBase(matchdoc::ReferenceBase* ref, std::vector<Row>* rows)
        : _ref(ref)
        , _rows(rows)
    {}
    virtual ~ColumnDataBase() {}
public:
    std::string toString(size_t rowIndex) const {
        std::string str = _ref->toString((*_rows)[rowIndex]);
        if (_ref->meta.valueType.isMultiValue()) {
            autil::StringUtil::replaceAll(str, std::string(1, autil::MULTI_VALUE_DELIMITER), "^]");
        }
        return str;
    }
    std::string toOriginString(size_t rowIndex) const {
        return _ref->toString((*_rows)[rowIndex]);
    }

    inline size_t getRowCount() const {
        return _rows->size();
    }
private:
    matchdoc::ReferenceBase* _ref;
protected:
    std::vector<Row>* _rows;
};

template <typename T>
class ColumnData : public ColumnDataBase
{
public:
    ColumnData(matchdoc::Reference<T>* ref, std::vector<Row>* rows)
        : ColumnDataBase(ref, rows)
        , _ref(ref)
    {}
    ~ColumnData() {}
public:
    inline const T& get(size_t rowIndex) const;
    inline const T& get(const Row &row) const;

    inline T* getPointer(size_t rowIndex) const;
    inline T* getPointer(const Row &row) const;

    inline void set(size_t rowIndex, const T& value);
    inline void set(const Row &row, const T& value);
private:
    matchdoc::Reference<T>* _ref;
};

template<typename T>
inline const T& ColumnData<T>::get(size_t rowIndex) const {
    assert(rowIndex < _rows->size());
    return *_ref->getPointer((*_rows)[rowIndex]);
}

template<typename T>
inline const T& ColumnData<T>::get(const Row &row) const {
    return *_ref->getPointer(row);
}

template<typename T>
inline  T* ColumnData<T>::getPointer(size_t rowIndex) const {
    assert(rowIndex < _rows->size());
    return _ref->getPointer((*_rows)[rowIndex]);
}

template<typename T>
inline T* ColumnData<T>::getPointer(const Row &row) const {
    return _ref->getPointer(row);
}

template<typename T>
inline void ColumnData<T>::set(size_t rowIndex, const T& value) {
    assert(rowIndex < _rows->size());
    _ref->set((*_rows)[rowIndex], value);
}

template<typename T>
inline void ColumnData<T>::set(const Row &row, const T& value) {
    _ref->set(row, value);
}

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_COLUMN_DATA_H
