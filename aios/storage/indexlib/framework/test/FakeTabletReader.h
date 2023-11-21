#pragma once

#include "indexlib/framework/TabletReader.h"

namespace indexlibv2::framework {

class FakeTabletReader final : public TabletReader
{
public:
    FakeTabletReader(const std::string& tabletName) : TabletReader(nullptr), _tabletName(tabletName) {}

    Status DoOpen(const std::shared_ptr<TabletData>& tabletData, const ReadResource& readResource) override
    {
        return Status::OK();
    }

    const std::string& TabletName() const { return _tabletName; }

private:
    const std::string _tabletName;
};

} // namespace indexlibv2::framework
