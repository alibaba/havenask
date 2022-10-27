#ifndef ISEARCH_BS_SHAREDPTRGUARD_H
#define ISEARCH_BS_SHAREDPTRGUARD_H

#include "build_service/common_define.h"
#include <autil/Lock.h>

namespace build_service {
namespace util {

template <typename T>    
class SharedPtrGuard
{
public:
    SharedPtrGuard()
    {} 
    
    SharedPtrGuard(const std::shared_ptr<T>& dataPtr)
        : _dataPtr(dataPtr)
    {}
    ~SharedPtrGuard() {};

private:
    SharedPtrGuard(const SharedPtrGuard &);
    SharedPtrGuard& operator=(const SharedPtrGuard &);

public:
    void set(const std::shared_ptr<T>& inDataPtr) {
        autil::ScopedLock lock(_mutex);
        _dataPtr = inDataPtr;
    }

    void get(std::shared_ptr<T>& outDataPtr) const {
        autil::ScopedLock lock(_mutex);
        outDataPtr = _dataPtr;
    }

    void reset() {
        autil::ScopedLock lock(_mutex);
        _dataPtr.reset();
    }
        
private:
    std::shared_ptr<T> _dataPtr;
    mutable autil::ThreadMutex _mutex;    
};


}
}

#endif //ISEARCH_BS_SHAREDPTRGUARD_H
