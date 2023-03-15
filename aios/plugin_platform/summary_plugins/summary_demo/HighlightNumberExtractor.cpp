#include <summary_demo/HighlightNumberExtractor.h>

using namespace std;
using namespace autil;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(summary);
namespace pluginplatform {
namespace summary_plugins {
HA3_LOG_SETUP(summary_plugins, HighlightNumberExtractor);

HighlightNumberExtractor::HighlightNumberExtractor(
        const vector<string> &attrNames) 
    : _attrNames(attrNames)
    , _configVec(NULL)
{ 
}

HighlightNumberExtractor::HighlightNumberExtractor(
        const HighlightNumberExtractor &other)
{
    _attrNames = other._attrNames;
    _configVec = other._configVec;
    _keywords = other._keywords;
}

HighlightNumberExtractor::~HighlightNumberExtractor() { 
}

void HighlightNumberExtractor::extractSummary(isearch::common::SummaryHit &summaryHit) {
    FieldSummaryConfig defaultConfig;
    for (summaryfieldid_t fieldId = 0; 
         fieldId < (summaryfieldid_t)summaryHit.getFieldCount(); ++fieldId)
    {
        const ConstString *inputStr = summaryHit.getFieldValue(fieldId);
        if (!inputStr) {
            continue;
        }
        const FieldSummaryConfig *configPtr = NULL;
        if ((size_t)fieldId < _configVec->size()) {
            configPtr = (*_configVec)[fieldId]; 
        }
        if (configPtr == NULL) {
            configPtr = &defaultConfig;
        }
        string output;
        string input(inputStr->data(), inputStr->size());
        highlightNumber(input, output, configPtr);
        summaryHit.setFieldValue(fieldId, output.data(), output.size());
    }
}

void HighlightNumberExtractor::highlightNumber(
        const string &input, string &output, 
        const FieldSummaryConfig *configPtr)
{
    size_t beginPos = 0;
    bool foundNumber = false;
    for (size_t i = 0; i < input.size(); i++) {
        if ('0' <= input[i] && input[i] <= '9') {
            foundNumber = true;
            continue;
        }
        if (foundNumber) {
            output += configPtr->_highlightPrefix;
            for (size_t j = beginPos; j < i; j++) {
                output += input[j];
            }
            output += configPtr->_highlightSuffix;
        }
        output += input[i];
        foundNumber = false;
        beginPos = i+1;
    }
    if (foundNumber) {
        output += configPtr->_highlightPrefix;
        for (size_t j = beginPos; j < input.size(); j++) {
            output += input[j];
        }
        output += configPtr->_highlightSuffix;
    }
}

bool HighlightNumberExtractor::beginRequest(SummaryExtractorProvider *provider) {
    _configVec = provider->getFieldSummaryConfig();
    const SummaryQueryInfo *queryInfo = provider->getQueryInfo();
    const vector<isearch::common::Term> &terms = queryInfo->terms;
    _keywords.clear();
    for (size_t i = 0; i < terms.size(); i++) {
        _keywords.insert(string(terms[i].getWord().c_str()));
    }
    for (size_t i = 0; i < _attrNames.size(); i++) {
        summaryfieldid_t summaryFieldId = 
            provider->fillAttributeToSummary(_attrNames[i]);
        if (summaryFieldId == INVALID_SUMMARYFIELDID) {
            // this field is already in use
            return false;
        }
    }
    return true;
}
SummaryExtractor* HighlightNumberExtractor::clone() {
    return new HighlightNumberExtractor(*this);
}

void HighlightNumberExtractor::destory() {
    delete this;
}

}}

