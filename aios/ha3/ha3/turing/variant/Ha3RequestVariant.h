#ifndef ISEARCH_HA3REQUESTVARIANT_H
#define ISEARCH_HA3REQUESTVARIANT_H

#include <autil/Log.h>
#include <tensorflow/core/framework/variant.h>
#include <tensorflow/core/framework/variant_encode_decode.h>
#include <ha3/common/Request.h>


BEGIN_HA3_NAMESPACE(turing);
class Ha3RequestVariant
{
public:
    Ha3RequestVariant();
    Ha3RequestVariant(autil::mem_pool::Pool *pool);
    Ha3RequestVariant(HA3_NS(common)::RequestPtr request, autil::mem_pool::Pool *pool);
    Ha3RequestVariant(const Ha3RequestVariant &other);
    ~Ha3RequestVariant();
public:
    void Encode(tensorflow::VariantTensorData* data) const;
    bool Decode(const tensorflow::VariantTensorData& data);
    std::string TypeName() const {
        return "Ha3Request";
    }
    HA3_NS(common)::RequestPtr getRequest() const { return _request; }
    bool construct(autil::mem_pool::Pool *outPool);
private:
    autil::mem_pool::Pool* _externalPool;
    HA3_NS(common)::RequestPtr _request;
    std::string _metadata;
private:
    AUTIL_LOG_DECLARE();
};

END_HA3_NAMESPACE(turing);
#endif //ISEARCH_HA3REQUESTVARIANT_H
