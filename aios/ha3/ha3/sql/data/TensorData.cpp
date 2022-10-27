#include <ha3/sql/data/TensorData.h>
#include "tensorflow/core/framework/tensor.pb.h"
using namespace std;
using namespace autil;
BEGIN_HA3_NAMESPACE(sql);

TensorData::TensorData(autil::mem_pool::Pool *pool)
    : _pool(pool)
{}
TensorData::TensorData(tensorflow::Tensor tensor, autil::mem_pool::Pool *pool)
    : _tensor(tensor)
    , _pool(pool)
{}

void TensorData::serialize(autil::DataBuffer &dataBuffer) const {
    tensorflow::TensorProto proto;
    _tensor.AsProtoField(&proto);
    string protoStr;
    proto.SerializeToString(&protoStr);
    dataBuffer.write(protoStr);
}

bool TensorData::deserialize(autil::DataBuffer &dataBuffer) {
    string protoStr;
    dataBuffer.read(protoStr);
    tensorflow::TensorProto proto;
    proto.ParseFromString(protoStr);
    if (proto.dtype() != tensorflow::DT_VARIANT) {
        return false;
    }
    return _tensor.FromProto(proto);
}

void TensorData::serializeToString(std::string &data, autil::mem_pool::Pool *pool) const {
    DataBuffer dataBuffer(DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, pool);
    serialize(dataBuffer);
    data.clear();
    data.append(dataBuffer.getData(), dataBuffer.getDataLen());
}

bool TensorData::deserializeFromString(const std::string &data, autil::mem_pool::Pool *pool) {
    DataBuffer dataBuffer((void*)data.c_str(), data.size(), pool);
    return deserialize(dataBuffer);
}

END_HA3_NAMESPACE(sql);
