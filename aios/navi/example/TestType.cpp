#include "navi/example/TestType.h"
#include "navi/example/TestData.h"
#include <iostream>

namespace navi {

CharArrayType::CharArrayType()
    : Type(__FILE__, CHAR_ARRAY_TYPE_ID)
{
}

CharArrayType::~CharArrayType() {
}

TypeErrorCode CharArrayType::serialize(TypeContext &ctx,
                                       const DataPtr &data) const
{
    if (!data) {
        return TEC_NULL_DATA;
    }
    auto helloData = dynamic_cast<HelloData *>(data.get());
    if (!helloData) {
        return TEC_TYPE_MISMATCH;
    }
    auto &dataBuffer = ctx.getDataBuffer();
    dataBuffer.write(helloData);
    return TEC_NONE;
}

TypeErrorCode CharArrayType::deserialize(TypeContext &ctx,
                                         DataPtr &data) const
{
    HelloData *helloData = nullptr;
    auto &dataBuffer = ctx.getDataBuffer();
    dataBuffer.read(helloData);
    if (!helloData) {
        return TEC_NULL_DATA;
    }
    data.reset(helloData);
    return TEC_NONE;
}


REGISTER_TYPE(CharArrayType);
REGISTER_TYPE(IntArrayType);

}

