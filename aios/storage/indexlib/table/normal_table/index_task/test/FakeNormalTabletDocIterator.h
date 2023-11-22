#pragma once

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/table/normal_table/NormalTabletDocIterator.h"

namespace indexlibv2::config {
class FieldConfig;
};

namespace indexlibv2::table {

class FakeNormalTabletDocIterator : public framework::ITabletDocIterator
{
public:
    FakeNormalTabletDocIterator(const std::shared_ptr<config::ITabletSchema>& targetSchema);
    ~FakeNormalTabletDocIterator();

public:
    indexlibv2::Status Init(const std::shared_ptr<indexlibv2::framework::TabletData>& tabletData,
                            std::pair<uint32_t /*0-99*/, uint32_t /*0-99*/> rangeInRatio,
                            const std::shared_ptr<indexlibv2::framework::MetricsManager>& metricsManager,
                            const std::optional<std::vector<std::string>>& requiredFields,
                            const std::map<std::string, std::string>& params) override;
    indexlibv2::Status Next(indexlibv2::document::RawDocument* rawDocument, std::string* checkpoint,
                            indexlibv2::framework::Locator::DocInfo* docInfo) override;
    bool HasNext() const override;
    indexlibv2::Status Seek(const std::string& checkpoint) override;

private:
    bool GetSourceField(std::shared_ptr<indexlibv2::config::FieldConfig> fieldConfig, std::string& fieldName);
    bool UseDefaultValue(std::shared_ptr<indexlibv2::config::FieldConfig> fieldConfig);

private:
    std::shared_ptr<indexlibv2::framework::ITabletDocIterator> _docIter;
    std::shared_ptr<config::ITabletSchema> _schema;
    std::map<std::string, std::string> _fieldMapper;
    std::map<std::string, std::string> _defaultValues;
    ;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
