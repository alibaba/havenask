#include "indexlib/partition/flushed_locator_container.h"
#include <chrono>

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, FlushedLocatorContainer);

FlushedLocatorContainer::FlushedLocatorContainer(size_t size)
    : mSize(size)
{
}

FlushedLocatorContainer::~FlushedLocatorContainer() 
{
}

void FlushedLocatorContainer::Push(std::future<bool>& future,
                                   const document::Locator& locator)
{
    ScopedLock l(mLock);
    if (mLocatorQueue.size() >= mSize)
    {
        mLocatorQueue.pop_front();
    }
    mLocatorQueue.push_back( {future.share(), locator} );
}

document::Locator FlushedLocatorContainer::GetLastFlushedLocator()
{
    ScopedLock l(mLock);
    for (auto iter = mLocatorQueue.rbegin(); iter != mLocatorQueue.rend(); ++iter)
    {
        if (IsReady(*iter) && iter->first.get())
        {
            mCachedLocator = iter->second;
            break;
        }
    }
    return mCachedLocator;
}

bool FlushedLocatorContainer::HasFlushingLocator()
{
    ScopedLock l(mLock);
    for (auto iter = mLocatorQueue.rbegin(); iter != mLocatorQueue.rend(); ++iter)
    {
        if (!IsReady(*iter))
        {
            return true;
        }
    }
    return false;
}

void FlushedLocatorContainer::Clear()
{
    ScopedLock l(mLock);
    mLocatorQueue.clear();
}

bool FlushedLocatorContainer::IsReady(const LocatorFuture& locatorFuture) const
{
    const std::shared_future<bool>& f = locatorFuture.first;
    if (!f.valid())
    {
        IE_LOG(ERROR, "locatorFuture[%s] is not valid",
               locatorFuture.second.ToString().c_str());
        return false;
    }

    bool isReady = (f.wait_for(std::chrono::seconds(0)) ==
                    std::future_status::ready);
    return isReady;
}

IE_NAMESPACE_END(partition);
