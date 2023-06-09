/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/codec/Normalizer.h"
#include "ha3/config/SummaryProfileInfo.h"
#include "ha3/summary/SummaryExtractor.h"
#include "indexlib/indexlib.h"

namespace isearch {
namespace common {
class SummaryHit;
class Term;
}  // namespace common
namespace summary {
class SummaryExtractorProvider;
}  // namespace summary
}  // namespace isearch

namespace isearch {
namespace summary {

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
    autil::codec::NormalizerPtr _normalizerPtr;
    const config::FieldSummaryConfigVec *_configVec;
    std::vector<std::string> _keywords;
    int64_t _sleepTime;
    summaryfieldid_t _fieldIdforIp;
private:
    friend class DefaultSummaryExtractorTest;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<DefaultSummaryExtractor> DefaultSummaryExtractorPtr;

} // namespace summary
} // namespace isearch
