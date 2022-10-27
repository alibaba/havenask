#ifndef ISEARCH_UTIL_SERIALIZE_H
#define ISEARCH_UTIL_SERIALIZE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <autil/DataBuffer.h>

#define HA3_DATABUFFER_READ(member) dataBuffer.read(member);
#define HA3_DATABUFFER_WRITE(member) dataBuffer.write(member);

#define HA3_SERIALIZE_DECLARE(...)                                      \
    void serialize(autil::DataBuffer &dataBuffer) const __VA_ARGS__;    \
    void deserialize(autil::DataBuffer &dataBuffer) __VA_ARGS__;

#define HA3_SERIALIZE_IMPL(ClassName, MEMBERS_MACRO)                    \
    void ClassName::serialize(autil::DataBuffer &dataBuffer) const       \
    { MEMBERS_MACRO(HA3_DATABUFFER_WRITE); }                            \
    void ClassName::deserialize(autil::DataBuffer &dataBuffer)           \
    { MEMBERS_MACRO(HA3_DATABUFFER_READ); }

#define HA3_SERIALIZE_DECLARE_AND_IMPL(MEMBERS_MACRO)           \
    void serialize(autil::DataBuffer &dataBuffer) const          \
    { MEMBERS_MACRO(HA3_DATABUFFER_WRITE); }                    \
    void deserialize(autil::DataBuffer &dataBuffer)              \
    { MEMBERS_MACRO(HA3_DATABUFFER_READ); }

#endif //ISEARCH_UTIL_SERIALIZE_H
