#ifndef __INDEXLIB_BASE_CLASS_HPP
#define __INDEXLIB_BASE_CLASS_HPP

#include "indexlib/misc/test/base_class.h"
#include "indexlib/misc/test/childA_class.h"
#include "indexlib/misc/test/childB_class.h"

IE_NAMESPACE_BEGIN(misc);

/* final function */
inline void __ALWAYS_INLINE BaseClass::printImpl() 
{
    IE_CALL_VIRTUAL_FUNC(BaseClass, this, print);
}

IE_NAMESPACE_END(misc);

#endif //__INDEXLIB_DERIVE_CLASS_HPP
