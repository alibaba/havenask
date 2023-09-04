#pragma once

#include "indexlib/table/test/TableTestHelper.h"

namespace indexlibv2::framework {
class Locator;
}

namespace indexlibv2 { namespace table {
class KVTableTestHelper : public TableTestHelper
{
public:
    KVTableTestHelper(bool autoCleanIndex = true, bool needDeploy = true) : TableTestHelper(autoCleanIndex, needDeploy)
    {
    }
    ~KVTableTestHelper() = default;

    Status DoBuild(std::string docs, bool oneBatch = false) override;
    Status DoBuild(const std::string& docStr, const framework::Locator& locator) override;

    bool DoQuery(std::string indexType, std::string indexName, std::string queryStr, std::string expectValue) override;
    static bool DoQuery(const std::shared_ptr<framework::ITabletReader>& reader, std::string indexType,
                        std::string indexName, std::string queryStr, std::string expectValue);

public:
    static std::shared_ptr<config::ITabletSchema>
    MakeSchema(const std::string& fieldNames, const std::string& keyName, const std::string& valueNames,
               int64_t ttl = -1, const std::string& valueFormat = "" /*valueFormat = plain|impact*/);
    static std::shared_ptr<indexlibv2::config::ITabletSchema> LoadSchema(const std::string& dir,
                                                                         const std::string& schemaFileName);

protected:
    std::shared_ptr<TableTestHelper> CreateMergeHelper() override;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlibv2::table
