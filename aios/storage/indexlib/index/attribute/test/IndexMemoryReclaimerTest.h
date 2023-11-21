#pragma once

#include "indexlib/framework/mem_reclaimer/IIndexMemoryReclaimer.h"

namespace indexlibv2::index {

class IndexMemoryReclaimerTest : public framework::IIndexMemoryReclaimer
{
public:
    IndexMemoryReclaimerTest(bool releaseImmediately = true) : _retireId(0), mReleaseImmediately(releaseImmediately) {}
    ~IndexMemoryReclaimerTest() = default;

    struct RetireItem {
        int64_t retireId = 0;
        void* addr = nullptr;
        std::function<void(void*)> deAllocator;
    };

public:
    int64_t Retire(void* addr, std::function<void(void*)> deAllocator) override
    {
        if (mReleaseImmediately) {
            deAllocator(addr);
        } else {
            std::lock_guard<std::mutex> guard(_dataMutex);
            mToReleaseMems.push_back({_retireId, addr, deAllocator});
            _retireId++;
        }
        return _retireId - 1;
    }
    void DropRetireItem(int64_t itemId) override
    {
        std::lock_guard<std::mutex> guard(_dataMutex);
        for (auto iter = mToReleaseMems.begin(); iter != mToReleaseMems.end(); iter++) {
            if (iter->retireId == itemId) {
                mToReleaseMems.erase(iter);
                return;
            }
        }
    }
    void TryReclaim() override { return Reclaim(); }
    void Reclaim() override
    {
        std::vector<RetireItem> tmpReleaseMems;
        {
            std::lock_guard<std::mutex> guard(_dataMutex);
            tmpReleaseMems = mToReleaseMems;
            mToReleaseMems.clear();
        }
        for (auto i = 0; i < tmpReleaseMems.size(); i++) {
            tmpReleaseMems[i].deAllocator(tmpReleaseMems[i].addr);
        }
    }

private:
    int64_t _retireId;
    bool mReleaseImmediately;
    std::vector<RetireItem> mToReleaseMems;
    mutable std::mutex _dataMutex;
};

} // namespace indexlibv2::index
