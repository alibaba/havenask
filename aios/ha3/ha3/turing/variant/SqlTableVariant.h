#ifndef ISEARCH_SQLTABLEVARIANT_H
#define ISEARCH_SQLTABLEVARIANT_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sql/data/Table.h>
#include <tensorflow/core/framework/variant.h>
#include <tensorflow/core/framework/tensor.h>
#include <tensorflow/core/framework/variant_encode_decode.h>

BEGIN_HA3_NAMESPACE(turing);

class SqlTableVariant
{
public:
    SqlTableVariant();
    SqlTableVariant(const std::shared_ptr<autil::mem_pool::Pool> &poolPtr);
    SqlTableVariant(sql::TablePtr table, autil::mem_pool::Pool *pool)
        : _table(table)
        , _pool(pool)
        , _flag(0)
    {
    }
    SqlTableVariant(const SqlTableVariant &);
    ~SqlTableVariant();
private:
    SqlTableVariant& operator=(const SqlTableVariant &);
public:
    void Encode(tensorflow::VariantTensorData* data) const;
    bool Decode(const tensorflow::VariantTensorData& data);
    bool construct(const std::shared_ptr<autil::mem_pool::Pool> &tablePool,
                   autil::mem_pool::Pool *dataPool);
    std::string TypeName() const {
        return "SqlTable";
    }
    bool isEmptyTable() { return _flag == char(1); }

public:
    sql::TablePtr getTable() const {
        return _table;
    }
private:
    sql::TablePtr _table;
    autil::mem_pool::Pool* _pool;
    std::string _metadata;
    char _flag;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SqlTableVariant);

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_SQLTABLEVARIANT_H
