#ifndef __INDEXLIB_FLUSHED_LOCATOR_CONTAINER_H
#define __INDEXLIB_FLUSHED_LOCATOR_CONTAINER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/document/locator.h"
#include <autil/Lock.h>
#include <deque>
#include <future>

IE_NAMESPACE_BEGIN(partition);

class FlushedLocatorContainer
{
public:
    FlushedLocatorContainer(size_t size);
    ~FlushedLocatorContainer();
public:
    void Push(std::future<bool>& future, const document::Locator& locator);
    document::Locator GetLastFlushedLocator();
    bool HasFlushingLocator();
    void Clear();

private:
    typedef std::pair<std::shared_future<bool>, document::Locator> LocatorFuture;

private:
    bool IsReady(const LocatorFuture& locatorFuture) const;

private:
    size_t mSize;
    std::deque<LocatorFuture> mLocatorQueue;
    autil::ThreadMutex mLock;

    document::Locator mCachedLocator;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FlushedLocatorContainer);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_FLUSHED_LOCATOR_CONTAINER_H
