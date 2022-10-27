#ifndef ISEARCH_DEFAULTSUMMARYEXTRACTOR_H
#define ISEARCH_DEFAULTSUMMARYEXTRACTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <vector>
#include <string>
#include <utility>
#include <tr1/memory>
#include <ha3/summary/SummaryExtractor.h>
#include <build_service/analyzer/Normalizer.h>

BEGIN_HA3_NAMESPACE(summary);

class DefaultSummaryExtractor : public SummaryExtractor
{
public:
    DefaultSummaryExtractor();
    ~DefaultSummaryExtractor() {}
public:
    bool beginRequest(SummaryExtractorProvider *provider) override;
    void extractSummary(common::SummaryHit &summaryHit) override;
    SummaryExtractor* clone() override;
    void destory() override;
private:
    std::string getSummary(const std::string &text, 
                           const std::vector<std::string>& keywords,
                           const config::FieldSummaryConfig *configPtr);
private:
    size_t findLastUtf8Character(const std::string &text,
                                 size_t pos);

    size_t findNextUtf8Character(const std::string &text, 
                                 size_t pos);
private:
    struct PosInfo {
        PosInfo(size_t pos = 0, size_t len = 0, size_t keywordIndex = 0)
            : pos(pos), len(len), keywordIndex(keywordIndex) {}
        size_t pos;
        size_t len;
        size_t keywordIndex;
    };
    
private:
    void getKeywordsFromTerms(const std::vector<common::Term> &terms);
    std::string searchKeywordPosition(std::vector<PosInfo> &posVec,
            const std::string &text, 
            const std::vector<std::string> &keywords);
    
    void findMaxUniqueKeyWindow(size_t &windowBeginPos, 
                                size_t &windowEndPos, 
                                const std::vector<PosInfo> &posVec, 
                                const std::vector<std::string> &keywords,
                                size_t summaryLen);

    void extendSentenceSeparator(size_t &windowBeginPos, 
                                 size_t &windowEndPos, 
                                 size_t summaryLen, 
                                 const std::string &text);

    std::string highlight(size_t windowBeginPos,
                          size_t windowEndPos,
                          const std::vector<PosInfo> &posVec,
                          const std::string &text, 
                          const std::vector<std::string>& keywords,
                          const std::string &highlightPrefix,
                          const std::string &highlightSuffix);

    std::string getFixedSummary(const std::string &text,
                                const std::string &snippetDelimiter,
                                size_t summaryLen);

    void findAllSeparators(const std::string &text, 
                           std::vector<size_t> &separatorPosVec);

private:
    build_service::analyzer::NormalizerPtr _normalizerPtr;
    const config::FieldSummaryConfigVec *_configVec;
    std::string _queryString;
    std::vector<std::string> _keywords;
    int64_t _sleepTime;
    summaryfieldid_t _fieldIdforIp;
private:
    friend class DefaultSummaryExtractorTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(DefaultSummaryExtractor);

END_HA3_NAMESPACE(summary);

#endif //ISEARCH_DEFAULTSUMMARYEXTRACTOR_H
