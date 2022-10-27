#ifndef ISEARCH_HA3RESULTVARIANT_H
#define ISEARCH_HA3RESULTVARIANT_H

#include <autil/Log.h>
#include <tensorflow/core/framework/variant.h>
#include <tensorflow/core/framework/tensor.h>
#include <tensorflow/core/framework/variant_encode_decode.h>
#include <ha3/common/Result.h>


BEGIN_HA3_NAMESPACE(turing);
class Ha3ResultVariant
{
public:
    Ha3ResultVariant();
    Ha3ResultVariant(autil::mem_pool::Pool *pool);
    Ha3ResultVariant(HA3_NS(common)::ResultPtr result, autil::mem_pool::Pool *pool);
    Ha3ResultVariant(const Ha3ResultVariant &other);
    ~Ha3ResultVariant();
public:
    void Encode(tensorflow::VariantTensorData* data) const;
    bool Decode(const tensorflow::VariantTensorData& data);
    std::string TypeName() const {
        return "Ha3Result";
    }
    HA3_NS(common)::ResultPtr getResult() const { return _result; }
    bool construct(autil::mem_pool::Pool *outPool);
private:
    autil::mem_pool::Pool* _externalPool;
    HA3_NS(common)::ResultPtr _result;
    std::string _metadata;
private:
    AUTIL_LOG_DECLARE();
};

END_HA3_NAMESPACE(turing);
#endif //ISEARCH_HA3RESULTVARIANT_H
