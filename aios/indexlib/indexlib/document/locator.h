#ifndef __INDEXLIB_LOCATOR_H
#define __INDEXLIB_LOCATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include <autil/ConstString.h>
#include <autil/mem_pool/Pool.h>
#include <autil/DataBuffer.h>
#include <autil/StringUtil.h>
#include <autil/Lock.h>

IE_NAMESPACE_BEGIN(document);

class Locator;
class ThreadSafeLocator;

class Locator 
{
public:
    Locator(autil::mem_pool::Pool* pool = NULL);
    ~Locator();
    explicit Locator(const std::string &locator,
                     autil::mem_pool::Pool* pool = NULL);
    explicit Locator(const autil::ConstString &locator,
                     autil::mem_pool::Pool* pool = NULL);
    Locator(const Locator &other);
    Locator(const ThreadSafeLocator &other);

public:
    void SetLocator(const std::string &locator);
    void SetLocator(const autil::ConstString &locator);
    const autil::ConstString& GetLocator() const;
    bool operator==(const Locator &other) const;
    bool operator!=(const Locator &locator) const;
    void operator=(const Locator &other);
    void operator=(const ThreadSafeLocator &other);
    bool IsValid() const;
    std::string ToString() const;
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer,
                     autil::mem_pool::Pool* pool);

private:
    void CopyLocatorStr(const void* data, size_t size);
    void ReleaseBuffer();
    char* AllocBuffer(size_t size);

private:
    autil::ConstString mLocatorStr;
    char* mBuffer;
    size_t mCapacity;
    autil::mem_pool::Pool* mPool;
};

class ThreadSafeLocator 
{
public:
    ThreadSafeLocator(autil::mem_pool::Pool* pool = NULL);
    explicit ThreadSafeLocator(const std::string &locator,
                               autil::mem_pool::Pool* pool = NULL);
    explicit ThreadSafeLocator(const autil::ConstString &locator,
                               autil::mem_pool::Pool* pool = NULL);
    ThreadSafeLocator(const ThreadSafeLocator &other);
    ThreadSafeLocator(const Locator &other);

public:
    void SetLocator(const std::string &locator);
    void SetLocator(const autil::ConstString &locator);
    const autil::ConstString& GetLocator() const;
    bool operator==(const ThreadSafeLocator &other) const;
    bool operator==(const Locator &other) const;
    bool operator!=(const ThreadSafeLocator &other) const;
    bool operator!=(const Locator &other) const;
    void operator=(const Locator &other);
    void operator=(const ThreadSafeLocator &other);
    bool IsValid() const;

    std::string ToString() const;
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer,
                     autil::mem_pool::Pool* pool);

private:
    Locator mLocator;
    mutable autil::ThreadMutex mLock;
private:
    friend class Locator;
};

    
DEFINE_SHARED_PTR(Locator);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_LOCATOR_H
