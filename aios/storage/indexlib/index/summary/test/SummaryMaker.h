#pragma once

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/document/normal/SummaryDocument.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/summary/SummaryMemIndexer.h"

namespace indexlibv2::index {

class SummaryMaker : private autil::NoCopyable
{
public:
    using DocumentArray = std::vector<std::shared_ptr<indexlib::document::SummaryDocument>>;

public:
    SummaryMaker() = default;
    ~SummaryMaker() = default;

public:
    static std::shared_ptr<indexlib::document::SummaryDocument>
    MakeOneDocument(docid_t docId, const std::shared_ptr<config::ITabletSchema>& schema, autil::mem_pool::Pool* pool);

    static Status BuildOneSegment(const std::shared_ptr<indexlib::file_system::IDirectory>& segDirectory,
                                  segmentid_t segId, std::shared_ptr<config::ITabletSchema>& schema, uint32_t docCount,
                                  autil::mem_pool::Pool* pool, DocumentArray& answerDocArray);

    static std::shared_ptr<SummaryMemIndexer>
    BuildOneSegmentWithoutDump(uint32_t docCount, const std::shared_ptr<config::ITabletSchema>& schema,
                               autil::mem_pool::Pool* pool, DocumentArray& answerDocArray);

    static IIndexFactory* GetIndexFactory(const std::shared_ptr<indexlibv2::config::IIndexConfig>& config);
    static std::shared_ptr<IIndexReader> CreateIndexReader(std::shared_ptr<framework::TabletData>& tabletData,
                                                           const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                           IndexerParameter& indexerParam);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
