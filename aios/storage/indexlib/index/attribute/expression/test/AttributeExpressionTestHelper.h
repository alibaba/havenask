#include "indexlib/base/FieldTypeUtil.h"
#include "indexlib/document/extractor/plain/DocumentInfoExtractorFactory.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/index/attribute/AttributeIndexFactory.h"
#include "indexlib/index/attribute/AttributeMemIndexer.h"
#include "indexlib/index/attribute/SingleValueAttributeMemIndexer.h"
#include "indexlib/index/attribute/expression/TabletSessionResource.h"
#include "indexlib/index/attribute/test/AttributeTestUtil.h"

namespace indexlibv2::index {

template <FieldType ft, bool isMulti>
class FakeMemSegment : public framework::Segment
{
public:
    FakeMemSegment(std::shared_ptr<AttributeMemIndexer> indexer)
        : Segment(framework::Segment::SegmentStatus::ST_BUILDING)
        , _indexer(indexer)
    {
    }
    std::pair<Status, std::shared_ptr<indexlibv2::index::IIndexer>> GetIndexer(const std::string& type,
                                                                               const std::string& indexName) override
    {
        return {Status::OK(), _indexer};
    }
    void AddOneDoc(typename indexlib::IndexlibFieldType2CppType<ft, isMulti>::CppType value)
    {
        docid_t docid = _segmentMeta.segmentInfo->docCount;
        autil::mem_pool::Pool pool;
        std::string fieldValue = std::to_string(value);
        auto encodeStr = _indexer->_attrConvertor->Encode(autil::StringView(fieldValue), &pool);
        _indexer->AddField(docid, encodeStr, false);
        _segmentMeta.segmentInfo->docCount++;
    }
    size_t EstimateMemUsed(const std::shared_ptr<config::ITabletSchema>& schema) override { return 0; }
    size_t EvaluateCurrentMemUsed() override { return 0; }

private:
    std::shared_ptr<AttributeMemIndexer> _indexer;
};

template <FieldType ft, bool isMulti>
class AttributeExpressionTestHelper
{
public:
    AttributeExpressionTestHelper() {}
    virtual ~AttributeExpressionTestHelper() {}
    bool Init()
    {
        _docInfoExtractorFactory = std::make_shared<indexlibv2::plain::DocumentInfoExtractorFactory>();
        AttributeIndexFactory indexFactory;
        IndexerParameter indexParam;
        _config = AttributeTestUtil::CreateAttrConfig<ft>(isMulti, "");
        auto memIndexer = indexFactory.CreateMemIndexer(_config, indexParam);
        if (!memIndexer->Init(_config, _docInfoExtractorFactory.get()).IsOK()) {
            return false;
        }
        _memIndexer = std::dynamic_pointer_cast<AttributeMemIndexer>(memIndexer);
        if (!_memIndexer) {
            return false;
        }
        _memSegment.reset(new FakeMemSegment<ft, isMulti>(_memIndexer));
        _segments.clear();
        _segments.push_back(std::dynamic_pointer_cast<framework::Segment>(_memSegment));
        _resource.reset(new TabletSessionResource(&_pool, _segments));
        return true;
    }
    void AddOneDoc(uint32_t value) { _memSegment->AddOneDoc(value); }

public:
    std::shared_ptr<document::extractor::IDocumentInfoExtractorFactory> _docInfoExtractorFactory;
    std::shared_ptr<AttributeMemIndexer> _memIndexer;
    std::shared_ptr<FakeMemSegment<ft, isMulti>> _memSegment;
    std::shared_ptr<AttributeConfig> _config;
    autil::mem_pool::Pool _pool;
    std::vector<std::shared_ptr<framework::Segment>> _segments;
    std::shared_ptr<TabletSessionResource> _resource;
};

} // namespace indexlibv2::index
