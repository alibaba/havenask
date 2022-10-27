#ifndef __INDEXLIB_DUMPABLE_H
#define __INDEXLIB_DUMPABLE_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>

IE_NAMESPACE_BEGIN(file_system);

class Dumpable
{
public:
    Dumpable() {}
    virtual ~Dumpable() {}
public:
    virtual void Dump() = 0;
};

DEFINE_SHARED_PTR(Dumpable);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_DUMPABLE_H
