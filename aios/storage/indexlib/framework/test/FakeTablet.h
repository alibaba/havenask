#pragma once
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/framework/Tablet.h"
#include "indexlib/framework/mock/MockTabletFactory.h"

namespace indexlibv2::framework {

MockTabletFactory* factory = nullptr;

class FakeTablet : public Tablet
{
public:
    FakeTablet(const framework::TabletResource& res) : Tablet(res) {}

    std::unique_ptr<framework::ITabletFactory>
    CreateTabletFactory(const std::string& tableType,
                        const std::shared_ptr<config::TabletOptions>& options) const override
    {
        return std::unique_ptr<MockTabletFactory>(factory);
    }
    std::shared_ptr<config::ITabletSchema> GetTabletSchema() const override
    {
        if (_schema) {
            return _schema;
        }
        return Tablet::GetTabletSchema();
    }

    void SetSchema(std::shared_ptr<config::ITabletSchema> schema) { _schema = schema; }

private:
    std::shared_ptr<config::ITabletSchema> _schema;
};

} // namespace indexlibv2::framework
