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
#include "indexlib/index/statistics_term/StatisticsTermFormatter.h"

#include "autil/Log.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"

namespace indexlibv2::index {
AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.index, StatisticsTermFormatter);

static constexpr char SEP = '\t';
static constexpr char SUFFIX = '\n';

std::string StatisticsTermFormatter::GetHeaderLine(const std::string& invertedIndexName, size_t termCount)
{
    return invertedIndexName + SEP + std::to_string(termCount) + SUFFIX;
}

std::string StatisticsTermFormatter::GetDataLine(const std::string& term, size_t termHash)
{
    return std::to_string(termHash) + SEP + std::to_string(term.size()) + SEP + term + SUFFIX;
}

Status StatisticsTermFormatter::ParseHeaderLine(const std::string& line, std::string& indexName, size_t& termCount)
{
    const std::vector<std::string> tokens = autil::StringTokenizer::tokenize(line, SEP);
    if (tokens.size() != 2) {
        AUTIL_LOG(ERROR, "fail to parse header line, data[%s]", line.c_str());
        return Status::InternalError("parse failed");
    }
    indexName = tokens[0];
    termCount = autil::StringUtil::fromString<size_t>(tokens[1]);
    return Status::OK();
}

Status StatisticsTermFormatter::ParseDataLine(const std::string& block, size_t& pos, std::string& term,
                                              size_t& termHash)
{
    auto termHashPos = block.find_first_of(SEP, pos);
    if (termHashPos == std::string::npos) {
        AUTIL_LOG(ERROR, "extract term hash failed, pos[%lu]", pos);
        return Status::InternalError("extract term hash failed");
    }
    auto termHashStr = block.substr(pos, termHashPos - pos);
    termHash = autil::StringUtil::fromString<size_t>(termHashStr);

    auto termLenPos = block.find_first_of(SEP, termHashPos + 1);
    if (termLenPos == std::string::npos) {
        AUTIL_LOG(ERROR, "extract term len failed");
        return Status::InternalError("extract term str failed");
    }

    // termHash \t termStrLen \t term \n
    auto termLenStr = block.substr(termHashPos + 1, termLenPos - termHashPos - 1);
    size_t termLen = autil::StringUtil::fromString<size_t>(termLenStr);

    term = block.substr(termLenPos + 1, termLen);

    pos = termLenPos + /*\t*/ 1 + termLen + /*\n*/ 1;
    return Status::OK();
}
} // namespace indexlibv2::index
