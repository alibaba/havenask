#pragma once

#include <memory>

#include "indexlib/table/test/TableTestHelper.h"

namespace indexlib::document {
class NormalDocument;
}
namespace indexlib::config {
class IndexPartitionSchema;
}
namespace indexlibv2::index::ann {
class AithetaAuxSearchInfoBase;
}

namespace indexlibv2 { namespace table {
class NormalTableTestHelper : public TableTestHelper
{
public:
    NormalTableTestHelper(bool autoCleanIndex = true, bool needDeploy = true,
                          framework::OpenOptions::BuildMode buildMode = framework::OpenOptions::STREAM);
    ~NormalTableTestHelper();

    void AfterOpen() override;
    Status DoBuild(std::string docs, bool oneBatch = true) override;
    Status DoBuild(const std::string& docStr, const framework::Locator& locator) override;
    bool DoQuery(std::string indexType, std::string indexName, std::string queryStr, std::string expectValue) override;
    // std::string Query(std::string indexType, std::string indexName, std::string queryStr) override;

public:
    static bool Query(const std::shared_ptr<framework::ITabletReader>& tabletReader, const std::string& queryStr,
                      const std::string& expectResultStr, bool ignoreDeletionMap = false);
    bool Query(const std::string& queryStr, const std::string& expectResultStr, bool ignoreDeletionMap = false);
    bool QueryANN(const std::string& queryStr, const std::string& expectResultStr, bool ignoreDeletionMap = false,
                  std::shared_ptr<indexlibv2::index::ann::AithetaAuxSearchInfoBase> searchInfo = nullptr);
    docid_t QueryPK(const std::string& pk);
    Status SpecifySegmentsMerge(const std::vector<segmentid_t>& srcSegIds);
    std::pair<Status, framework::VersionCoord> OfflineSpecifySegmentsMerge(const framework::VersionCoord& versionCoord,
                                                                           const std::vector<segmentid_t>& srcSegIds);

public:
    static std::shared_ptr<config::TabletSchema> MakeSchema(const std::string& fieldNames,
                                                            const std::string& indexNames,
                                                            const std::string& attributeNames,
                                                            const std::string& summaryNames);

protected:
    std::shared_ptr<TableTestHelper> CreateMergeHelper() override;

private:
    void AddAlterTabletResourceIfNeed(std::vector<std::shared_ptr<framework::IndexTaskResource>>& extendResources,
                                      const std::shared_ptr<config::ITabletSchema>& schema) override;
    std::shared_ptr<document::IDocumentBatch> CreateNormalDocumentBatch(const std::string& docs);
    std::shared_ptr<document::IDocumentBatch> CreateNormalDocumentBatchWithLocator(const std::string& docStr,
                                                                                   const framework::Locator& locator);
    bool RewriteDocumentBatch(const std::shared_ptr<config::ITabletSchema>& schema,
                              const std::shared_ptr<document::IDocumentBatch>& docBatch);
    Status InitTypedTabletResource() override;

public:
    inline static const std::string INDEX_TERM_SEPARATOR = ":";
    inline static const std::string POSTING_TYPE_SEPARATOR = "$"; // 用法：helper.Query("130$bt_bitmap", ...)
    // inline static const std::string TERM_SEPARATOR = ",";
    inline static const std::string DOCID = "__docid";
    inline static const std::string SUBDOCID = "__subdocid";
    inline static const std::string NULL_TERM = "IS_NULL";

private:
    size_t _nextDocTimestamp = 0;

    framework::OpenOptions::BuildMode _buildMode;
    std::shared_ptr<autil::ThreadPool> _buildThreadPool;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlibv2::table
