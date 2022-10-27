#ifndef ISEARCH_TENSORDATAVARIANT_H
#define ISEARCH_TENSORDATAVARIANT_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sql/data/TensorData.h>
#include <tensorflow/core/framework/variant.h>
#include <tensorflow/core/framework/tensor.h>
#include <tensorflow/core/framework/variant_encode_decode.h>

BEGIN_HA3_NAMESPACE(turing);

class TensorDataVariant
{
public:
    TensorDataVariant();
    TensorDataVariant(autil::mem_pool::Pool *pool);
    TensorDataVariant(sql::TensorDataPtr tensorData, autil::mem_pool::Pool *pool)
        : _tensorData(tensorData)
        , _pool(pool)
    {}
    TensorDataVariant(const TensorDataVariant &);
    ~TensorDataVariant();
private:
    TensorDataVariant& operator=(const TensorDataVariant &);
public:
    void Encode(tensorflow::VariantTensorData* data) const;
    bool Decode(const tensorflow::VariantTensorData& data);
    bool construct(autil::mem_pool::Pool *outPool);
    std::string TypeName() const {
        return "TensorData";
    }
public:
    sql::TensorDataPtr getTensorData() const {
        return _tensorData;
    }
private:
    sql::TensorDataPtr _tensorData;
    autil::mem_pool::Pool* _pool;
    std::string _metadata;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(TensorDataVariant);

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_SQLTABLEVARIANT_H
