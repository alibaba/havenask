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

#include <stdint.h>
#include <string>

#include "build_service/analyzer/Token.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "suez/turing/expression/provider/common.h"

namespace suez {
namespace turing {

class TermMetaInfo {
public:
    TermMetaInfo() {
        _df = 0;
        _tf = 0;
        _payload = 0;
        _matchDataLevel = MDL_TERM;
    }
    TermMetaInfo(indexlib::index::TermMeta *termMeta) { setTermMeta(termMeta); }
    ~TermMetaInfo() {}

public:
    inline df_t getDocFreq() const { return _df; }
    inline tf_t getTotalTermFreq() const { return _tf; }
    inline termpayload_t getPayload() const { return _payload; }
    inline int32_t getTermBoost() const { return _boost; }
    inline std::string getIndexName() const { return _indexName; }
    inline std::string getTruncateName() const { return _truncateName; }
    inline const std::string &getTermText() const { return _token.getNormalizedText(); }
    void
    setTermInfo(build_service::analyzer::Token token, std::string indexName, int32_t boost, std::string truncateName) {
        _token = token;
        _indexName = indexName;
        _boost = boost;
        _truncateName = truncateName;
    }
    void setTermMeta(indexlib::index::TermMeta *termMeta) {
        _df = termMeta->GetDocFreq();
        _tf = termMeta->GetTotalTermFreq();
        _payload = termMeta->GetPayload();
    }
    void setMatchDataLevel(MatchDataLevel level) { _matchDataLevel = level; }
    void setQueryLabel(const std::string &label) { _queryLabel = label; }
    MatchDataLevel getMatchDataLevel() const { return _matchDataLevel; }
    const std::string &getQueryLabel() const { return _queryLabel; }

private:
    df_t _df;
    tf_t _tf;
    termpayload_t _payload;
    MatchDataLevel _matchDataLevel;
    std::string _queryLabel;

    build_service::analyzer::Token _token;
    std::string _indexName;
    int32_t _boost;
    std::string _truncateName;
};

} // namespace turing
} // namespace suez
