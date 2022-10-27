#ifndef ISEARCH_SQLRESULTVARIANT_H
#define ISEARCH_SQLRESULTVARIANT_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <tensorflow/core/framework/variant.h>
#include <tensorflow/core/framework/tensor.h>
#include <tensorflow/core/framework/variant_encode_decode.h>
#include <navi/engine/NaviResult.h>

BEGIN_HA3_NAMESPACE(turing);

class SqlResultVariant
{
public:
    SqlResultVariant()
        : _pool(nullptr)
    {}
    SqlResultVariant(const navi::NaviResultPtr &result, autil::mem_pool::Pool *pool)
        : _result(result)
        , _pool(pool)
    {}
private:
    SqlResultVariant& operator=(const SqlResultVariant &);
public:
    void Encode(tensorflow::VariantTensorData* data) const;
    bool Decode(const tensorflow::VariantTensorData& data);
    bool construct(autil::mem_pool::Pool *outPool);
    std::string TypeName() const {
        return "SqlResult";
    }
    navi::NaviResultPtr getResult() {
        return _result;
    }
private:
    navi::NaviResultPtr _result;
    autil::mem_pool::Pool *_pool;
    std::string _metadata;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SqlResultVariant);

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_SQLRESULTVARIANT_H
