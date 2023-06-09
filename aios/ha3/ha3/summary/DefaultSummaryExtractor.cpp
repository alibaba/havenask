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
#include "ha3/summary/DefaultSummaryExtractor.h"

#include <assert.h>
#include <unistd.h>
#include <algorithm>
#include <memory>
#include <sstream>

#include "alog/Logger.h"
#include "autil/ConstString.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/codec/CodeConverter.h"
#include "autil/codec/NormalizeOptions.h"
#include "autil/codec/Normalizer.h"
#include "indexlib/analyzer/AnalyzerDefine.h"
#include "build_service/analyzer/Token.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/Request.h"
#include "ha3/common/SummaryHit.h"
#include "ha3/common/Tracer.h"
#include "ha3/summary/SummaryExtractor.h"
#include "ha3/summary/SummaryExtractorProvider.h"
#include "ha3/util/NetFunction.h"

namespace isearch {
namespace common {
class Term;
}  // namespace common
}  // namespace isearch

using namespace std;
using namespace autil;
using namespace autil::codec;
using namespace build_service::analyzer;
using namespace isearch::util;
using namespace isearch::config;

namespace isearch {
namespace summary {
AUTIL_LOG_SETUP(ha3, DefaultSummaryExtractor);

DefaultSummaryExtractor::DefaultSummaryExtractor() {
    _normalizerPtr.reset(new Normalizer(NormalizeOptions(false, false, false)));
    _sleepTime = 0;
    _fieldIdforIp = (summaryfieldid_t)-1;
}

SummaryExtractor* DefaultSummaryExtractor::clone() {
    return new DefaultSummaryExtractor(*this);
}

void DefaultSummaryExtractor::destory() {
    delete this;
}

bool DefaultSummaryExtractor::beginRequest(SummaryExtractorProvider *provider) {
    _configVec = provider->getFieldSummaryConfig();
    const SummaryQueryInfo *queryInfo = provider->getQueryInfo();
    getKeywordsFromTerms(queryInfo->terms);

    REQUEST_TRACE_WITH_TRACER(provider->getRequestTracer(),
                              TRACE2, "DefaultSummaryExtractor begin request");
    const common::Request *request = provider->getRequest();
    common::ConfigClause *configClause = request->getConfigClause();
    if (configClause != NULL) {
        string sleepTimeStr = configClause->getKVPairValue("summary_extract_sleep_time");
        int64_t sleepTime;
        if (StringUtil::fromString<int64_t>(sleepTimeStr, sleepTime)) {
            _sleepTime = sleepTime;
        }
        string sleepIpStr = configClause->getKVPairValue("sleep_ip");
        string myIp;
        util::NetFunction::getPrimaryIP(myIp);
        if (!sleepIpStr.empty() && sleepIpStr != myIp) {
            _sleepTime = 0;
        }
        if (_sleepTime > 0) {
            AUTIL_LOG(INFO, "sleep time: [%ld]ms", _sleepTime);
        }

        bool fillIp = false;
        string fillIpStr = configClause->getKVPairValue("fill_ip");
        if (StringUtil::fromString<bool>(fillIpStr, fillIp) && fillIp) {
            _fieldIdforIp = provider->declareSummaryField("ip");
            AUTIL_LOG(INFO, "ip will be filled in summary hit");
        }
    }
    return true;
}

void DefaultSummaryExtractor::getKeywordsFromTerms(const vector<common::Term> &terms)
{
    _keywords.clear();
    _keywords.resize(terms.size());
    for (size_t i = 0; i < terms.size(); ++i) {
        if (terms[i].getToken().isStopWord()) {
            continue;
        }
        string ss;
        _normalizerPtr->normalize(terms[i].getWord(), ss);
        _keywords[i].assign(ss.c_str());
    }
}

void DefaultSummaryExtractor::extractSummary(common::SummaryHit &summaryHit) {
    if (_sleepTime > 0) {
        usleep(_sleepTime * 1000);
    }
    if (_fieldIdforIp != (summaryfieldid_t)-1) {
        string ip;
        if (util::NetFunction::getPrimaryIP(ip)) {
            summaryHit.setFieldValue(_fieldIdforIp, ip.c_str(), ip.length());
        }
    }
    FieldSummaryConfig defaultConfig;
    size_t count = summaryHit.getFieldCount();
    for (size_t summaryFieldId = 0; summaryFieldId < count; ++summaryFieldId) {
        const StringView *inputStr = summaryHit.getFieldValue(summaryFieldId);
        if (!inputStr) {
            continue;
        }
        const SummaryFieldInfo *summaryFieldInfo = summaryHit.getSummaryFieldInfo(summaryFieldId);
        const FieldSummaryConfig* fieldSummaryConfig =
            summaryFieldId < _configVec->size() ? (*_configVec)[summaryFieldId] : NULL;
        if (fieldSummaryConfig || summaryFieldInfo->fieldType == ft_text) {
            const FieldSummaryConfig *configPtr = fieldSummaryConfig ? fieldSummaryConfig : &defaultConfig;
            string input(inputStr->data(), inputStr->size());
            string output = getSummary(input, _keywords, configPtr);
            summaryHit.setFieldValue(summaryFieldId, output.data(), output.size());
        }
    }
}

string DefaultSummaryExtractor::getSummary(const string &oriText,
        const vector<string>& keywords,
        const FieldSummaryConfig *configPtr)
{
    size_t summaryLen = configPtr->_maxSummaryLength;
    if (summaryLen == 0) {
        AUTIL_LOG(WARN, "the 'summaryLen' you want is zero");
        return "";
    }

    //find all keywords' position
    vector<PosInfo> posVec;
    string text = searchKeywordPosition(posVec, oriText, keywords);
    if (posVec.size() == 0) {
        AUTIL_LOG(TRACE3, "not find any keywords");
        return getFixedSummary(text, configPtr->_snippetDelimiter, summaryLen);
    }

    //find the window included the max unique keyword count in source text.
    size_t windowBeginPos = 0;
    size_t windowEndPos = 0;
    findMaxUniqueKeyWindow(windowBeginPos, windowEndPos, posVec, keywords, summaryLen);
    if (windowEndPos <= windowBeginPos) {
        AUTIL_LOG(TRACE3, "all keywords' length is greater than 'summaryLen'.");
        return getFixedSummary(text, configPtr->_snippetDelimiter, summaryLen);
    }

    //extend window to left and right sentence separator;
    // at the same time ensure window's size is not greater than 'summaryLen'
    extendSentenceSeparator(windowBeginPos, windowEndPos, summaryLen, text);

    //highlight the keyword in window
    string finalText = highlight(windowBeginPos, windowEndPos,
                                 posVec, text, keywords,
                                 configPtr->_highlightPrefix, configPtr->_highlightSuffix);
    if (windowEndPos < (text.size() - 1)) {
        return finalText + configPtr->_snippetDelimiter;
    } else {
        return finalText;
    }
}

string DefaultSummaryExtractor::searchKeywordPosition(vector<PosInfo> &posVec,
        const string &text,
        const vector<string> &keywords)
{
    vector<PosInfo> termVec;
    termVec.reserve(text.size() / 4);
    for (size_t i = 0; i < text.size();) {
        size_t b = i;
        while(b < text.size()
              && text[b] != '\t')
        {
            ++b;
        }

        if (b > i) {
            termVec.push_back(PosInfo(i, b - i));
        }
        i = b + 1;
    }

    posVec.reserve(256);

    string ret;
    ret.reserve(text.size());
    for (size_t i = 0; i < termVec.size(); ++i) {
        string ori(text.c_str() + termVec[i].pos,
                        termVec[i].len);
        string ss;
        _normalizerPtr->normalize(ori, ss);
        for (size_t j = 0; j < keywords.size(); ++j) {
            if (ss == keywords[j]) {
                posVec.push_back(PosInfo(ret.size(), termVec[i].len, j));
            }
        }
        ret.append(ori.c_str(), ori.size());
    }

    return ret;
}

void DefaultSummaryExtractor::findMaxUniqueKeyWindow(size_t &windowBeginPos,
        size_t &windowEndPos,
        const vector<PosInfo> &posVec,
        const vector<string> &keywords,
        size_t summaryLen)
{
    vector<int> keywordOccVec(keywords.size(), 0);

    size_t startIndex = 0;//keyword start index in posVec
    size_t endIndex = 0;//keyword end index in posVec
    int curUniqueKeyCount = 0;//the unique key count in current window
    int maxUniqueKeyCount = 0;//the max unique key count

    size_t posVecSize = posVec.size();
    for (; endIndex < posVecSize; ++endIndex) {
        const PosInfo& curPosInfo = posVec[endIndex];

        //adjust the current window's unique keyword count
        int rightKeywordIndex = curPosInfo.keywordIndex;
        keywordOccVec[rightKeywordIndex]++;
        if (1 == keywordOccVec[rightKeywordIndex]) {
            curUniqueKeyCount++;
        }

        //ensure the current window length is less than 'Summarylen'
        size_t curWindowBeginPos = posVec[startIndex].pos;
        size_t curWindowEndPos = curPosInfo.pos + curPosInfo.len;

        while(curWindowEndPos > curWindowBeginPos + summaryLen)
        {
            int &leftKeywordOcc = keywordOccVec[posVec[startIndex].keywordIndex];
            leftKeywordOcc--;
            if (leftKeywordOcc == 0) {
                curUniqueKeyCount--;
            }
            ++startIndex;
            if(startIndex <= endIndex) {
                curWindowBeginPos = posVec[startIndex].pos;
            } else {
                break;
            }
        }

        // have found the window with max keyword count and store the
        // window's start position, end position.
        if (curUniqueKeyCount > maxUniqueKeyCount) {
            maxUniqueKeyCount = curUniqueKeyCount;
            windowBeginPos = curWindowBeginPos;
            windowEndPos = curWindowEndPos;
        }
    }
}

string DefaultSummaryExtractor::highlight(size_t windowBeginPos,
        size_t windowEndPos, const vector<PosInfo> &posVec,
        const string &text,  const vector<string>& keywords,
        const string &highlightPrefix, const string &highlightSuffix)
{
    //the sub-string start position that need highlight
    size_t curHighlightBeginPos = windowBeginPos;
    //the sub-string end position that need highlight
    size_t curHighlightEndPos = windowBeginPos;


    //find the first keyword in the window
    size_t posVecSize = posVec.size();
    size_t i = 0;
    for(; i < posVecSize; ++i) {
        size_t keywordPos = posVec[i].pos;
        if (keywordPos >= windowBeginPos) {
            break;
        }
    }

    ostringstream oss;

    for (; i < posVecSize; ++i) {
        size_t keywordPos = posVec[i].pos;
        size_t keywordLen = posVec[i].len;
        if (keywordPos >= windowEndPos) {
            //the current keyword's position is beyond the window's right range
            break;
        }
        if (curHighlightEndPos >= windowEndPos) {
            //the highlight sub-string is out of the window
            break;
        }

        //the current keyword is overlaped with last highlight sub-string,
        // then extend highlight area
        if (keywordPos <= curHighlightEndPos) {
            curHighlightEndPos = max(curHighlightEndPos, keywordPos + keywordLen);
            continue;
        }

        //highlight the sub-string
        if (curHighlightEndPos > curHighlightBeginPos) {
            oss << highlightPrefix;
            oss << text.substr(curHighlightBeginPos, curHighlightEndPos - curHighlightBeginPos);
            oss << highlightSuffix;
        }

        //append none-highlight sub-string
        oss << text.substr(curHighlightEndPos, keywordPos - curHighlightEndPos);

        curHighlightBeginPos = keywordPos;
        curHighlightEndPos = keywordPos + keywordLen;
    }

    // handle the remained string
    assert(windowEndPos >= curHighlightBeginPos);
    assert(curHighlightEndPos >= curHighlightBeginPos);
    if (curHighlightEndPos >= windowEndPos) {//last sub-string is highlight area
        oss << highlightPrefix;
        oss << text.substr(curHighlightBeginPos, windowEndPos - curHighlightBeginPos);
        oss << highlightSuffix;
    } else {
        oss << highlightPrefix;
        oss << text.substr(curHighlightBeginPos, curHighlightEndPos - curHighlightBeginPos);
        oss << highlightSuffix;

        oss << text.substr(curHighlightEndPos, windowEndPos - curHighlightEndPos);
    }

    return oss.str();
}

static const string englishSeparators = " \n\t\r,.:;?!";
static const string chineseSeparators[] = {"，", "。", "：", "；", "？", "！"};

//the position stored in 'separatorPosVec' is the position after the separator string
void DefaultSummaryExtractor::findAllSeparators(const string &text, vector<size_t> &separatorPosVec)
{
    size_t pos = 0;
    for (;;) {
        pos = text.find_first_of(englishSeparators, pos);
        if (pos == string::npos) {
            break;
        }
        pos += 1; //all the English separator's length is 1
        separatorPosVec.push_back(pos);
    }

    size_t chineseSeparatorCount = sizeof(chineseSeparators) / sizeof(string);
    for (size_t i = 0; i < chineseSeparatorCount; ++i) {
        pos = 0;
        size_t separatorLength = chineseSeparators[i].size();
        for(;;) {
            pos = text.find(chineseSeparators[i], pos);
            if (pos == string::npos) {
                break;
            }
            pos += separatorLength;
            separatorPosVec.push_back(pos);
        }
    }

    sort(separatorPosVec.begin(), separatorPosVec.end());
}

void DefaultSummaryExtractor::extendSentenceSeparator(size_t &windowBeginPos,
        size_t &windowEndPos, size_t summaryLen, const string &text)
{
    assert(windowBeginPos + summaryLen >= windowEndPos);

    size_t extendLen = summaryLen - (windowEndPos - windowBeginPos);
    size_t prefixPos = windowBeginPos > extendLen ? windowBeginPos - extendLen : 0;
    size_t suffixPos = text.size() > extendLen + windowEndPos ? extendLen + windowEndPos : text.size();

    //handle multi-byte character
    prefixPos = findNextUtf8Character(text, prefixPos);
    suffixPos = findLastUtf8Character(text, suffixPos);

    string prefixText = text.substr(prefixPos, windowBeginPos - prefixPos);
    string suffixText = text.substr(windowEndPos, suffixPos - windowEndPos);

    vector<size_t> prefixSeparatorPosVec;
    vector<size_t> suffixSeparatorPosVec;
    if (prefixPos == 0) {
        //the begining of string is a special separator
        prefixSeparatorPosVec.push_back(0);
    }
    if (suffixPos == text.size()) {
        //the end of string is a special separator
        suffixSeparatorPosVec.push_back(suffixText.size());
    }
    findAllSeparators(prefixText, prefixSeparatorPosVec);
    findAllSeparators(suffixText, suffixSeparatorPosVec);

    size_t leftBasePos = prefixPos;
    size_t rightBasePos = windowEndPos;

    //extend to original string's head and tail in turn.
    vector<size_t>::reverse_iterator prefixIterator = prefixSeparatorPosVec.rbegin();
    vector<size_t>::iterator suffixIterator = suffixSeparatorPosVec.begin();
    while (prefixIterator != prefixSeparatorPosVec.rend()
           || suffixIterator != suffixSeparatorPosVec.end())
    {
        if (prefixIterator != prefixSeparatorPosVec.rend()) {
            size_t separatorPos = *prefixIterator;
            if (windowEndPos <= leftBasePos + separatorPos + summaryLen) {
                windowBeginPos = leftBasePos + separatorPos;
                ++prefixIterator;
            } else {
                //out of 'Summarylen' range, so does not extend to string's head
                prefixIterator = prefixSeparatorPosVec.rend();
            }
        }

        if (suffixIterator != suffixSeparatorPosVec.end()) {
            size_t separatorPos = *suffixIterator;
            if (windowBeginPos + summaryLen >= rightBasePos + separatorPos) {
                windowEndPos = rightBasePos + separatorPos;
                ++suffixIterator;
            } else {
                //out of 'Summarylen' range, so does not extend to string's tail
                suffixIterator = suffixSeparatorPosVec.end();
            }
        }
    }
}

string DefaultSummaryExtractor::getFixedSummary(const string &text,
        const string &snippetDelimiter, size_t summaryLen)
{
    if (text.size() <= summaryLen) {
        return text;
    }

    if (summaryLen == 0) {
        return "";
    }

    size_t pos = text.find_last_of(englishSeparators, summaryLen - 1);
    if (pos == string::npos) {
        //handle multi-bytes character.
        size_t length = findLastUtf8Character(text, summaryLen);
        return text.substr(0, length) + snippetDelimiter;
    }

    return text.substr(0, pos + 1) + snippetDelimiter;
}

size_t DefaultSummaryExtractor::findLastUtf8Character(const string &text, size_t pos)
{
    size_t i = pos;
    size_t size = text.size();

    if (i >= size) {
        return size;
    }

    while (i > 0 && (unsigned char)text[i] < 0xC0 && (unsigned char)text[i] >= 0x80) {
        i--;
    }

    return i;
}

size_t DefaultSummaryExtractor::findNextUtf8Character(const string &text, size_t pos)
{
    size_t i = pos;
    size_t size = text.size();

    if (i >= size) {
        return size;
    }

    while (i < size && (unsigned char)text[i] < 0xC0 && (unsigned char)text[i] >= 0x80) {
        i++;
    }

    return i;
}

} // namespace summary
} // namespace isearch
