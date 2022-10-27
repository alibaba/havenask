#ifndef ISEARCH_PBRESULTFORMATTER_H
#define ISEARCH_PBRESULTFORMATTER_H

#include <sstream>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <ha3/common/ResultFormatter.h>
#include <ha3/common/PBResult.pb.h>
#include <map>
#include <string>
BEGIN_HA3_NAMESPACE(common);

class PBResultFormatter : public ResultFormatter
{
public:
    const static uint32_t SUMMARY_IN_BYTES = 1;
    const static uint32_t PB_MATCHDOC_FORMAT = 2;
public:
    PBResultFormatter();
    ~PBResultFormatter();
public:
    void format(const ResultPtr &result, std::stringstream &ss);

    void format(const ResultPtr &result, PBResult &pbResult);

public:
    void setOption(uint32_t option) {
        _option = option;
    }
    void setAttrNameTransMap(const std::map<std::string, std::string>& attrNameMap) {
        _attrNameMap = attrNameMap;
    }
private:
    void formatHits(const ResultPtr &resultPtr, PBResult &pbResult);
    void formatMetaHits(const Hits *hits, PBHits *pbHits);
    void formatMeta(const ResultPtr &resultPtr, PBResult &pbResult);
    void formatHit(const HitPtr &hitPtr, PBHit *pbHit, bool isIndependentQuery);
    void formatMatchDocs(const ResultPtr &resultPtr, PBResult &pbResult);
    void formatSummary(const HitPtr &hitPtr, PBHit *pbHit);
    void formatProperty(const HitPtr &hitPtr, PBHit *pbHit);
    void formatAttributes(const HitPtr &hitPtr, PBHit *pbHit);
    void formatVariableValues(const HitPtr &hitPtr, PBHit *pbHit);
    void formatAggregateResults(const ResultPtr &resultPtr,
                                PBResult &pbResult);
    void formatAggregateResult(const AggregateResultPtr &aggResultPtr,
                               PBAggregateResults &pbAggregateResults);
    void formatErrorResult(const ResultPtr &resultPtr,
                           PBResult &pbResult);
    void formatTotalTime(const ResultPtr &resultPtr,
                         PBResult &pbResult);
    void formatRequestTracer(const ResultPtr &resultPtr,
                              PBResult &pbResult);
    void formatPBMatchDocVariableReferences(const matchdoc::ReferenceVector &refVec,
            const std::vector<matchdoc::MatchDoc> &matchDocs,
            PBMatchDocs *pbMatchDocs,
            ValueType valueType);
    void formatPBMatchDocSortValues(const std::vector<SortExprMeta> &sortExprMetas,
                                    const std::vector<matchdoc::MatchDoc> &matchDocs,
                                    PBMatchDocs *pbMatchDocs);
    template<typename T>
    double readNumericData2Double(const matchdoc::ReferenceBase *ref,
                                  const matchdoc::MatchDoc matchDoc) const;
private:
    void fillTypedValue(const std::string &key,
                        const AttributeItem *attrItem,
                        PBAttrKVPair *value);
private:
    uint32_t _option;
    autil::DataBuffer _dataBuffer;
    std::map<std::string, std::string> _attrNameMap;
private:
    friend class PBResultFormatterTest;

private:
    HA3_LOG_DECLARE();
};

template<typename T>
inline double PBResultFormatter::readNumericData2Double(
        const matchdoc::ReferenceBase *ref,
        const matchdoc::MatchDoc matchDoc) const
{
    auto typedRef = static_cast<const matchdoc::Reference<T> *>(ref);
    return typedRef->getReference(matchDoc);
}

END_HA3_NAMESPACE(common);


#endif //ISEARCH_PBRESULTFORMATTER_H
