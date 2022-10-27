#ifndef ISEARCH_LAYERMETASCREATOR_H
#define ISEARCH_LAYERMETASCREATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/QueryLayerClause.h>
#include <ha3/search/LayerMetas.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <ha3/common/RankHint.h>
BEGIN_HA3_NAMESPACE(search);

class LayerMetasCreator
{
public:
    LayerMetasCreator();
    ~LayerMetasCreator();
private:
    LayerMetasCreator(const LayerMetasCreator &);
    LayerMetasCreator& operator=(const LayerMetasCreator &);
public:
    void init(autil::mem_pool::Pool *pool,
              IndexPartitionReaderWrapper* readerPtr,
              const common::RankHint *rankHint = NULL);
    LayerMetas* create(const common::QueryLayerClause* layerClause) const;
private:
    LayerMetas* createLayerMetas(const common::QueryLayerClause* layerClause) const;
    LayerMetas* createDefaultLayerMetas() const;
    LayerMeta createDefaultLayerMeta() const;
    LayerMeta createFullRange() const;
    LayerMeta createLayerMeta(const common::LayerDescription *desc,
                              const LayerMeta &visitedRange) const;
private:
    LayerMeta convertSortAttrRange(
            const common::LayerDescription* layerDesc, int32_t from, 
            int32_t to,const LayerMeta& lastRange) const;
    LayerMeta convertRangeKeyWord(const common::LayerKeyRange *range, 
                                  LayerMeta &lastRange, 
                                  const LayerMeta &visitedRange) const;
private:
    LayerMeta segmentidLayerConvert(const common::LayerKeyRange *layerRange) const;
    LayerMeta docidLayerConvert(const common::LayerKeyRange *layerRange) const;
    LayerMeta orderedDocIdConvert() const;
    LayerMeta unOrderedDocIdConvert() const;
    LayerMeta percentConvert(const LayerMeta &lastRange,
                             const common::LayerKeyRange *range) const;
    LayerMeta otherConvert(const LayerMeta &lastRange,
                           const LayerMeta &visitedRange) const;
private:
    LayerMeta interSectRanges(const LayerMeta& layerMetaA,
                              const LayerMeta& layerMetaB) const;
private:
    QuotaMode decideQuotaMode(const common::LayerKeyRange *range,
                              size_t dimenCount, bool containUnSortedRange) const;
private:
    static int32_t convertNumberToRange(const std::string &numStr, int32_t maxNum);
    static IE_NAMESPACE(index_base)::DimensionDescriptionVector convert2DimenDescription(
            const common::LayerDescription* layerDesc, int32_t from, int32_t to);
    static bool needAggregate(const common::LayerDescription *desc);

private:
    autil::mem_pool::Pool *_pool;
    IndexPartitionReaderWrapper *_readerPtr;
    const common::RankHint *_rankHint;
private:
    HA3_LOG_DECLARE();
    friend class LayerMetasCreatorTest;
};

HA3_TYPEDEF_PTR(LayerMetasCreator);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_LAYERMETASCREATOR_H
