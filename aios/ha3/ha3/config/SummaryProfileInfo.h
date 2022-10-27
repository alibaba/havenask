#ifndef ISEARCH_SUMMARYPROFILEINFO_H
#define ISEARCH_SUMMARYPROFILEINFO_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/legacy/jsonizable.h>
#include <ha3/config/HitSummarySchema.h>

BEGIN_HA3_NAMESPACE(config);

const uint32_t DEFAULT_MAX_SNIPPET = 1;
const uint32_t DEFAULT_MAX_SUMMARY_LENGTH = 273;
const std::string DEFAULT_HIGHLIGHT_PREFIX = "<font color=red>";
const std::string DEFAULT_HIGHLIGHT_SUFFIX = "</font>";
const std::string DEFAULT_SNIPPET_DELIMITER = "...";

// FieldSummaryConfig
class FieldSummaryConfig : public autil::legacy::Jsonizable 
{
public:
    FieldSummaryConfig();
    ~FieldSummaryConfig();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
public:
    uint32_t _maxSnippet;
    uint32_t _maxSummaryLength;
    std::string _highlightPrefix;
    std::string _highlightSuffix;
    std::string _snippetDelimiter;
};
typedef std::map<std::string, FieldSummaryConfig> FieldSummaryConfigMap; 
typedef std::vector<FieldSummaryConfig*> FieldSummaryConfigVec;

// ExtractorInfo
class ExtractorInfo : public autil::legacy::Jsonizable
{
public:
    ExtractorInfo();
    ~ExtractorInfo();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
public:
    std::string _extractorName;
    std::string _moduleName;
    KeyValueMap _parameters;
};
typedef std::vector<ExtractorInfo> ExtractorInfoVector;

// SummaryProfileInfo
class SummaryProfileInfo : public autil::legacy::Jsonizable
{
public:
    SummaryProfileInfo();
    ~SummaryProfileInfo();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
public:
    void convertConfigMapToVector(const HitSummarySchema &hitSummarySchema);
public:
    std::string _summaryProfileName;
    ExtractorInfoVector _extractorInfos;
    FieldSummaryConfigMap _fieldSummaryConfigMap;
    FieldSummaryConfigVec _fieldSummaryConfigVec;
private:
    HA3_LOG_DECLARE();
};
typedef std::vector<SummaryProfileInfo> SummaryProfileInfos;

HA3_TYPEDEF_PTR(SummaryProfileInfo);

END_HA3_NAMESPACE(config);

#endif //ISEARCH_SUMMARYPROFILEINFO_H
