#ifndef NAVI_TESTTYPE_H
#define NAVI_TESTTYPE_H

#include "navi/engine/Type.h"

namespace navi {

static const std::string CHAR_ARRAY_TYPE_ID = "CharArrayType";

class CharArrayType : public Type {
public:
    CharArrayType();
    ~CharArrayType();
public:
    TypeErrorCode serialize(TypeContext &ctx,
                            const DataPtr &data) const override;
    TypeErrorCode deserialize(TypeContext &ctx, DataPtr &data) const override;
};

static const std::string INT_ARRAY_TYPE_ID = "IntArrayType";
class IntArrayType : public Type {
public:
    IntArrayType()
        : Type(__FILE__, INT_ARRAY_TYPE_ID)
    {
    }
    ~IntArrayType() {
    }
};

}

#endif //NAVI_TESTTYPE_H
