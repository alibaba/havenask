#pragma once

#include "indexlib/framework/mem_reclaimer/IIndexMemoryReclaimer.h"

namespace indexlibv2::index {

class IndexMemoryReclaimerTest : public framework::IIndexMemoryReclaimer
{
public:
    IndexMemoryReclaimerTest() = default;
    ~IndexMemoryReclaimerTest() = default;

public:
    void Retire(void* addr, std::function<void(void*)> deAllocator) override { deAllocator(addr); }
    void TryReclaim() override {}
};

} // namespace indexlibv2::index
