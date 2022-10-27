#ifndef __INDEXLIB_SINGLETON_H
#define __INDEXLIB_SINGLETON_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "autil/Lock.h"
#include <tr1/memory>

IE_NAMESPACE_BEGIN(misc);

class LazyInstantiation
{
protected:
    template<typename T>
    static void Create(T*& ptr)
    {
        ptr = new T;
        static std::tr1::shared_ptr<T> destroyer(ptr);
    }

};

template<typename T, typename InstPolicy = LazyInstantiation>
class Singleton : private InstPolicy
{
protected:
    Singleton(const Singleton&) {}
    Singleton(){}

public:
    ~Singleton(){}

public:
    /**
     * Provide access to the single instance through double-checked locking
     *
     * @return the single instance of object.
     */
    static T* GetInstance();

private:
    IE_LOG_DECLARE();
};

template<typename T, typename InstPolicy>
inline T* Singleton<T, InstPolicy>::GetInstance()
{
    static T* ptr = 0;
    static autil::RecursiveThreadMutex gLock;

    if (unlikely(!ptr))
    {
        autil::ScopedLock sl(gLock);
        if (!ptr)
        {
            InstPolicy::Create(ptr);
        }
    }
    return const_cast<T*>(ptr);
}

IE_NAMESPACE_END(misc);

#endif //__INDEXLIB_SINGLETON_H
