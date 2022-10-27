#ifndef ISEARCH_RESULTESTIMATOR_H
#define ISEARCH_RESULTESTIMATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/LayerMetas.h>
#include <ha3/search/Aggregator.h>

BEGIN_HA3_NAMESPACE(search);

class ResultEstimator
{
public:
    ResultEstimator();
    ~ResultEstimator();
private:
    ResultEstimator(const ResultEstimator &);
    ResultEstimator& operator=(const ResultEstimator &);
public:
    void init(const LayerMetas &allLayers, Aggregator *aggregator);
    void endLayer(uint32_t layer, uint32_t seekedCount,
                  uint32_t singleLayerMatchCount,
                  double truncateChainFactor);
    void endSeek();

    uint32_t getTotalMatchCount() const { return _totalMatchCount; }
    uint32_t getTotalSeekedCount() const { return _totalSeekedCount; }
    uint32_t getMatchCount() const { return _matchCount; }
    bool needAggregate(uint32_t layer) const { return _rangeSizes[layer] != 0; }

private:
    static bool isCovered(const LayerMeta &layer1, const LayerMeta &layer2);
    static void initLayerRangeSize(const LayerMetas &allLayers, 
                                   std::vector<uint32_t> &rangeSizes,
                                   std::vector<bool> &needAggregate);
    static void initCoveredInfo(const LayerMetas &allLayers, 
                                std::vector<std::vector<uint32_t> > &coveredLayerInfo);
private:
    std::vector<uint32_t> _rangeSizes;
    std::vector<bool> _needAggregate;
    uint32_t _totalSeekedLayerRange;
    uint32_t _totalSeekedCount;
    
    uint32_t _matchCount;
    uint32_t _totalMatchCount;
    Aggregator *_aggregator;

    std::vector<std::vector<uint32_t> > _coveredLayerInfo;
private:
    friend class ResultEstimatorTest;

private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(ResultEstimator);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_RESULTESTIMATOR_H
