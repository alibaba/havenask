#ifndef ISEARCH_SEEKITERATORVARIANT_H
#define ISEARCH_SEEKITERATORVARIANT_H

#include <ha3/common.h>
#include <ha3/common/TimeoutTerminator.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/SingleLayerSearcher.h>
#include <ha3/search/QueryExecutor.h>
#include <ha3/search/FilterWrapper.h>
#include <ha3/search/LayerMetas.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <ha3/turing/variant/ExpressionResourceVariant.h>
#include <matchdoc/MatchDocAllocator.h>
#include <indexlib/partition/index_partition_reader.h>
#include <indexlib/index/normal/attribute/accessor/join_docid_attribute_iterator.h>
#include <tensorflow/core/framework/variant.h>
#include <tensorflow/core/framework/tensor.h>
#include <tensorflow/core/framework/variant_encode_decode.h>

BEGIN_HA3_NAMESPACE(turing);

struct SeekIteratorParam {
    SeekIteratorParam()
        : layerMeta(NULL)
        , timeoutTerminator(NULL)
        , matchDataManager(NULL)
        , needAllSubDocFlag(false)
    {
    }
    search::IndexPartitionReaderWrapperPtr indexPartitionReaderWrapper;
    search::QueryExecutorPtr queryExecutor;
    search::FilterWrapperPtr filterWrapper;
    matchdoc::MatchDocAllocatorPtr matchDocAllocator;
    IE_NAMESPACE(index)::DeletionMapReaderPtr delMapReader;
    IE_NAMESPACE(index)::DeletionMapReaderPtr subDelMapReader;
    search::LayerMeta *layerMeta;
    IE_NAMESPACE(index)::JoinDocidAttributeIterator *mainToSubIt;
    common::TimeoutTerminator *timeoutTerminator;
    search::MatchDataManager *matchDataManager;
    bool needAllSubDocFlag;
};


class SeekIterator {
public:
    SeekIterator(const SeekIteratorParam &param) {
        _indexPartitionReaderWrapper = param.indexPartitionReaderWrapper;
        _queryExecutor = param.queryExecutor;
        _layerMeta = param.layerMeta;
        _filterWrapper = param.filterWrapper;
        _matchDocAllocator = param.matchDocAllocator;
        _delMapReader = param.delMapReader;
        _subDelMapReader = param.subDelMapReader;
        _needSubDoc = param.matchDocAllocator->hasSubDocAllocator();
        _singleLayerSearcher.reset(new search::SingleLayerSearcher(
                        _queryExecutor.get(), _layerMeta, _filterWrapper.get(), _delMapReader.get(),
                        _matchDocAllocator.get(), param.timeoutTerminator,
                        param.mainToSubIt, _subDelMapReader.get(), param.matchDataManager,
                        param.needAllSubDocFlag));
    }

    uint32_t getSeekTimes() const {
        return _singleLayerSearcher->getSeekTimes();
    }

    uint32_t getSeekDocCount() const {
        return _singleLayerSearcher->getSeekDocCount();
    }

    matchdoc::MatchDoc seek() {
        matchdoc::MatchDoc matchDoc;
        auto ec = _singleLayerSearcher->seek(_needSubDoc, matchDoc);
        if (unlikely(ec != IE_NAMESPACE(common)::ErrorCode::OK)) {
            if (ec == IE_NAMESPACE(common)::ErrorCode::FileIO) {
                return matchdoc::INVALID_MATCHDOC;
            }
            IE_NAMESPACE(common)::ThrowIfError(ec);
        }
        return matchDoc;
    }


    bool batchSeek(int32_t batchSize, std::vector<matchdoc::MatchDoc> &matchDocs) {
        int32_t docCount = 0;
        while (true) {
            matchdoc::MatchDoc doc = seek();
            if (matchdoc::INVALID_MATCHDOC == doc) {
                return true;
            }
            matchDocs.push_back(doc);
            docCount++;
            if (docCount >= batchSize) {
                return false;
            }
        }
    }

    matchdoc::MatchDocAllocatorPtr getMatchDocAllocator() const {
        return _matchDocAllocator;
    }

private:
    search::IndexPartitionReaderWrapperPtr _indexPartitionReaderWrapper;
    search::FilterWrapperPtr _filterWrapper;
    search::QueryExecutorPtr _queryExecutor;
    matchdoc::MatchDocAllocatorPtr _matchDocAllocator;
    IE_NAMESPACE(index)::DeletionMapReaderPtr _delMapReader;
    IE_NAMESPACE(index)::DeletionMapReaderPtr _subDelMapReader;
    search::LayerMeta *_layerMeta;
    search::SingleLayerSearcherPtr _singleLayerSearcher;
    bool _needSubDoc;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SeekIterator);

class SeekIteratorVariant
{
public:
    SeekIteratorVariant() {}
    SeekIteratorVariant(const SeekIteratorPtr &seekIterator,
                        const search::LayerMetasPtr &layerMetas,
                        const ExpressionResourcePtr &resource)
        : _seekIterator(seekIterator)
        , _layerMetas(layerMetas)
        , _expressionResource(resource)
    {
    }
    ~SeekIteratorVariant() {}

public:
    void Encode(tensorflow::VariantTensorData* data) const {}
    bool Decode(const tensorflow::VariantTensorData& data) {
        return false;
    }
    std::string TypeName() const {
        return "SeekIterator";
    }
public:
    SeekIteratorPtr getSeekIterator() const {
        return _seekIterator;
    }
private:
    SeekIteratorPtr _seekIterator;
    search::LayerMetasPtr _layerMetas;
    ExpressionResourcePtr _expressionResource;
private:
    HA3_LOG_DECLARE();

};

HA3_TYPEDEF_PTR(SeekIteratorVariant);

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_SEEKITERATORCLAUSEVARIANT_H
