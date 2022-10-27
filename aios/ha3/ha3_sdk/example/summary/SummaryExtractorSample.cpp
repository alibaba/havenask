#include <ha3_sdk/example/summary/SummaryExtractorSample.h>
#include <autil/StringUtil.h>
#include <sstream>

using namespace std;
using namespace autil;

USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(summary);
HA3_LOG_SETUP(summary, SummaryExtractorSample);

SummaryExtractorSample::SummaryExtractorSample(summaryfieldid_t newlyAddedFieldId) {
    assert(newlyAddedFieldId != INVALID_SUMMARYFIELDID);
    _newlyAddedFieldId = newlyAddedFieldId;
}

SummaryExtractorSample::~SummaryExtractorSample() {
}

SummaryExtractor* SummaryExtractorSample::clone() {
    return new SummaryExtractorSample(*this);
}

void SummaryExtractorSample::destory() {
    delete this;
}

bool SummaryExtractorSample::beginRequest(SummaryExtractorProvider *provider) {
    _configVec = provider->getFieldSummaryConfig();
    const SummaryQueryInfo *queryInfo = provider->getQueryInfo();
    const vector<common::Term> &terms = queryInfo->terms;
    _queryString = queryInfo->queryString;
    _keywords.clear();
    _keywords.reserve(terms.size());
    for (size_t i = 0; i < terms.size(); ++i) {
        _keywords.push_back(string(terms[i].getWord().c_str()));
    }
    return true;
}

void SummaryExtractorSample::extractSummary(common::SummaryHit &summaryHit) {
    FieldSummaryConfig defaultConfig;
    summaryfieldid_t summaryFieldId = 0;
    for (;summaryFieldId < (summaryfieldid_t)summaryHit.getFieldCount(); ++summaryFieldId) {
        const ConstString *inputStr = summaryHit.getFieldValue(summaryFieldId);
        if (!inputStr) {
            continue;
        }
        const FieldSummaryConfig *configPtr = (*_configVec)[summaryFieldId] ?
                                              (*_configVec)[summaryFieldId] : &defaultConfig;
        string output;
        if (_newlyAddedFieldId == summaryFieldId) {
            output = "user_define_value";
        } else {
            string input(inputStr->data(), inputStr->size());
            output = getSummary(input, _keywords, configPtr);
        }
        summaryHit.setFieldValue(summaryFieldId, output.data(), output.size());
    }
}

string SummaryExtractorSample::getSummary(const string &text,
        const vector<string>& keywords, const FieldSummaryConfig *configPtr)
{
    string summary = getAdjustedText(text, configPtr->_maxSummaryLength);
    vector<size_t> posVec;
    vector<PosInfo> posInfoVec;
    map<size_t,size_t> posMap;
    for (vector<string>::const_iterator it = keywords.begin(); it != keywords.end(); ++it) {
        autil::StringUtil::sundaySearch(summary, *it, posVec);
        for (size_t j = 0; j < posVec.size(); ++j) {
            if (posMap.find(posVec[j]) != posMap.end() && (*it).length() <= posMap[posVec[j]]) {
                continue;
            }
            posMap[posVec[j]] = (*it).length();
        }
    }
    return highlight(summary,posMap, configPtr->_highlightPrefix, configPtr->_highlightSuffix);
 }

string SummaryExtractorSample::getAdjustedText(const string &text, size_t summaryLen)
{

    const string noSpaceText = deleteSpaceCharacter(text);
    if (0 == summaryLen) {
        HA3_LOG(WARN, "the 'summaryLen' you want is zero");
        return "";
    }

    string summary = noSpaceText.substr(0,summaryLen);
    size_t len = summary.length();
    while (len > 0 && (unsigned char)summary[len-1] >= 0x80
           && (unsigned char)summary[len-1] < 0xC0)
    {
        len--;
    }
    if (0 == len) {
        return "";
    }

    if((unsigned char)summary[len-1] > 0xE0) {
        --len;
    }
    summary.resize(len);
    return summary;
}

string SummaryExtractorSample::highlight(const string &summary, map<size_t,size_t> &posMap,
        const string &highlightPrefix, const string &highlightSuffix)
{
    ostringstream oss;
    for (size_t i = 0; i<summary.length();) {
        if (posMap.find(i) != posMap.end()) {
            oss << highlightPrefix <<summary.substr(i,posMap[i]) << highlightSuffix;
            i += posMap[i];
        } else {
            oss << summary[i];
            ++i;
        }
    }
    return oss.str();
}

string SummaryExtractorSample::deleteSpaceCharacter(const string &str) {
    string ret;
    ret.reserve(str.size());
    for (size_t i = 0; i < str.size(); ++i) {
        if (str[i] != '\t' && str[i] != '\n' && str[i] != ' ') {
            ret.push_back(str[i]);
        }
    }
    return ret;
}

END_HA3_NAMESPACE(summary);

