#ifndef ISEARCH_TENSORDATA_H
#define ISEARCH_TENSORDATA_H
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <navi/engine/Data.h>
#include <autil/mem_pool/Pool.h>
#include <autil/DataBuffer.h>
#include <tensorflow/core/framework/tensor.h>

BEGIN_HA3_NAMESPACE(sql);

class TensorData : public navi::Data {
public:
    TensorData(autil::mem_pool::Pool *pool = NULL);
    TensorData(tensorflow::Tensor tensor, autil::mem_pool::Pool *pool);
private:
    TensorData(const TensorData &);
    TensorData& operator=(const TensorData &);
public:
    const tensorflow::Tensor& getTensor() const {
        return _tensor;
    }
    void serialize(autil::DataBuffer &dataBuffer) const;
    bool deserialize(autil::DataBuffer &dataBuffer);
    void serializeToString(std::string &data, autil::mem_pool::Pool *pool) const;
    bool deserializeFromString(const std::string &data, autil::mem_pool::Pool *pool);

private:
    tensorflow::Tensor _tensor;
    autil::mem_pool::Pool *_pool;
};
typedef std::shared_ptr<TensorData> TensorDataPtr;

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_TABLE_H
