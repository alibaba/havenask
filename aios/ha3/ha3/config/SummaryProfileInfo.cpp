#include <ha3/config/SummaryProfileInfo.h>
#include <ha3/config/TypeDefine.h>

using namespace std;
BEGIN_HA3_NAMESPACE(config);
HA3_LOG_SETUP(config, SummaryProfileInfo);

// FieldSummaryConfig
FieldSummaryConfig::FieldSummaryConfig() {
    _maxSnippet = DEFAULT_MAX_SNIPPET;
    _maxSummaryLength = DEFAULT_MAX_SUMMARY_LENGTH;
    _highlightPrefix = DEFAULT_HIGHLIGHT_PREFIX;
    _highlightSuffix = DEFAULT_HIGHLIGHT_SUFFIX;
    _snippetDelimiter = DEFAULT_SNIPPET_DELIMITER;
}

FieldSummaryConfig::~FieldSummaryConfig() {
}

void FieldSummaryConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    JSONIZE(json, "max_snippet", _maxSnippet);
    JSONIZE(json, "max_summary_length", _maxSummaryLength);
    JSONIZE(json, "highlight_prefix", _highlightPrefix);
    JSONIZE(json, "highlight_suffix", _highlightSuffix);
    JSONIZE(json, "snippet_delimiter", _snippetDelimiter);
}    

// ExtractorInfo
ExtractorInfo::ExtractorInfo() {
    _extractorName = "DefaultSummaryExtractor";
}

ExtractorInfo::~ExtractorInfo() {
}

void ExtractorInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    JSONIZE(json, "extractor_name", _extractorName);
    JSONIZE(json, "module_name", _moduleName);
    JSONIZE(json, "parameters", _parameters);
}

// SummaryProfileInfo
SummaryProfileInfo::SummaryProfileInfo() {
    _summaryProfileName = "DefaultProfile";
    _extractorInfos.push_back(ExtractorInfo());
}

SummaryProfileInfo::~SummaryProfileInfo() {
}

void SummaryProfileInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    JSONIZE(json, "summary_profile_name", _summaryProfileName);
    JSONIZE(json, "field_configs", _fieldSummaryConfigMap);
    if (json.GetMode() == FROM_JSON) {
        ExtractorInfoVector tmpExtractorInfos;
        JSONIZE(json, "extractors", tmpExtractorInfos);
        if (tmpExtractorInfos.size() == 0) {
            // compatible with old config
            ExtractorInfo extractorInfo;
            JSONIZE(json, "extractor", extractorInfo);
            tmpExtractorInfos.push_back(extractorInfo);
        }
        _extractorInfos.swap(tmpExtractorInfos);
    } else {
        JSONIZE(json, "extractors",  _extractorInfos);
    }
}

void SummaryProfileInfo::convertConfigMapToVector(
        const HitSummarySchema &hitSummarySchema) 
{
    _fieldSummaryConfigVec.resize(hitSummarySchema.getFieldCount());
    size_t count = hitSummarySchema.getFieldCount();
    for (size_t i = 0; i < count; ++i)
    {
        const config::SummaryFieldInfo *summaryFieldInfo = 
            hitSummarySchema.getSummaryFieldInfo(i);
        assert(summaryFieldInfo);
        FieldSummaryConfigMap::iterator mIt = 
            _fieldSummaryConfigMap.find(summaryFieldInfo->fieldName);
        if (mIt == _fieldSummaryConfigMap.end()) {
            _fieldSummaryConfigVec[i] = NULL;
        } else {
            _fieldSummaryConfigVec[i] = &mIt->second;
        }
    }
}

END_HA3_NAMESPACE(config);
