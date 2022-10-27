#ifndef __INDEXLIB_FSLIB_OPTION_H
#define __INDEXLIB_FSLIB_OPTION_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include <fslib/fs/IOController.h>

IE_NAMESPACE_BEGIN(storage);

enum IoAdvice
{
    IO_ADVICE_NORMAL = fslib::IOController::ADVICE_NORMAL,
    IO_ADVICE_LOW_LATENCY = fslib::IOController::ADVICE_LOW_LATENCY,
};

IE_NAMESPACE_END(storage);

#endif //__INDEXLIB_FSLIB_OPTION_H
