#pragma once

#include <memory>

#include "autil/Log.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/ann/aitheta2/AithetaTerm.h"

namespace autil::mem_pool {
class Pool;
}
namespace indexlibv2 {
namespace framework {
class ITabletReader;
}
namespace config {
class ITabletSchema;
}
namespace table {
class Result;
class Query;
class RawDocument;

class NormalTableTestSearcher
{
public:
    explicit NormalTableTestSearcher(const std::shared_ptr<framework::ITabletReader>& tabletReader,
                                     const std::shared_ptr<config::ITabletSchema>& tabletSchema);

public:
    std::shared_ptr<Result> Search(const std::string& queryStr, bool ignoreDeletionMap = false);
    std::shared_ptr<Result> SearchANN(const std::string& queryStr, bool ignoreDeletionMap = false,
                                      std::shared_ptr<index::ann::AithetaAuxSearchInfoBase> searchInfo = nullptr);

private:
    std::shared_ptr<Result> Search(std::shared_ptr<Query> query, bool ignoreDeletionMap);
    std::shared_ptr<Query> ParseQuery(const std::string& queryStr, autil::mem_pool::Pool* pool);
    std::shared_ptr<Query> ParseANNQuery(const std::string& queryStr, autil::mem_pool::Pool* pool,
                                         std::shared_ptr<index::ann::AithetaAuxSearchInfoBase> searchInfo = nullptr);
    void FillResult(std::shared_ptr<Query> query, docid_t docId, std::shared_ptr<Result> result, bool isDeleted);
    std::string GetAttributeValue(const std::string& attributeName, docid_t docId);
    std::string GetPackAttributeValue(const std::string& packName, const std::string& subAttributeName, docid_t docId);
    void GetSummaryValue(docid_t docId, std::shared_ptr<RawDocument>& rawDoc);
    void GetSourceValue(docid_t docId, std::shared_ptr<RawDocument>& rawDoc);

private:
    std::shared_ptr<framework::ITabletReader> _tabletReader;
    std::shared_ptr<config::ITabletSchema> _schema;
    std::string _primaryKeyIndexName;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace table
} // namespace indexlibv2
