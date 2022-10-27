#ifndef __INDEXLIB_DICTIONARY_TYPED_FACTORY_H
#define __INDEXLIB_DICTIONARY_TYPED_FACTORY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/misc/singleton.h"

IE_NAMESPACE_BEGIN(index);

template <typename ClassBase, template<typename T> class ClassTyped>
class DictionaryTypedFactory : public misc::Singleton<DictionaryTypedFactory<ClassBase, ClassTyped> >
{
protected:
    DictionaryTypedFactory() {}
    friend class misc::LazyInstantiation;
public:
    ~DictionaryTypedFactory() {}
public:
    ClassBase *Create(IndexType indexType)
    {
        if (indexType == it_number_int32)
        {
            return new ClassTyped<uint32_t>();
        }
        else if(indexType == it_number_uint32)
        {
            return new ClassTyped<uint32_t>();
        }
        else if(indexType == it_number_int64)
        {
            return new ClassTyped<uint64_t>();
        }
        else if(indexType == it_number_uint64)
        {
            return new ClassTyped<uint64_t>();
        }
        else if(indexType == it_number_int8)
        {
            return new ClassTyped<uint8_t>();
        }
        else if(indexType == it_number_uint8)
        {
            return new ClassTyped<uint8_t>();
        }
        else if(indexType == it_number_int16)
        {
            return new ClassTyped<uint16_t>();
        }
        else if(indexType == it_number_uint16)
        {
            return new ClassTyped<uint16_t>();
        }
        else if(indexType == it_unknown || indexType == it_number)
        {
            assert(false);
        }
        else
        {
            return new ClassTyped<dictkey_t>();
        }
        return NULL;
    }

    ClassBase *Create(IndexType indexType, autil::mem_pool::PoolBase* pool)
    {
        if (indexType == it_number_int32)
        {
            return new ClassTyped<uint32_t>(pool);
        }
        else if(indexType == it_number_uint32)
        {
            return new ClassTyped<uint32_t>(pool);
        }
        else if(indexType == it_number_int64)
        {
            return new ClassTyped<uint64_t>(pool);
        }
        else if(indexType == it_number_uint64)
        {
            return new ClassTyped<uint64_t>(pool);
        }
        else if(indexType == it_number_int8)
        {
            return new ClassTyped<uint8_t>(pool);
        }
        else if(indexType == it_number_uint8)
        {
            return new ClassTyped<uint8_t>(pool);
        }
        else if(indexType == it_number_int16)
        {
            return new ClassTyped<uint16_t>(pool);
        }
        else if(indexType == it_number_uint16)
        {
            return new ClassTyped<uint16_t>(pool);
        }
        else if(indexType == it_unknown || indexType == it_number)
        {
            assert(false);
        }
        else
        {
            return new ClassTyped<dictkey_t>(pool);
        }
        return NULL;
    }

private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DICTIONARY_TYPED_FACTORY_H
