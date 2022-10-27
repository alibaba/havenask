#ifndef __INDEXLIB_INDEXLIB_METRIC_CONTROL_H
#define __INDEXLIB_INDEXLIB_METRIC_CONTROL_H

#include <tr1/memory>
#include "autil/legacy/jsonizable.h"
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/singleton.h"
#include "indexlib/misc/regular_expression.h"

IE_NAMESPACE_BEGIN(misc);

class IndexlibMetricControl : public misc::Singleton<IndexlibMetricControl>
{
public:
    struct PatternItem : public autil::legacy::Jsonizable
    {
    public:
        PatternItem() : prohibit(false) {}
    public:
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
        {
            json.Jsonize("pattern", patternStr, patternStr);
            json.Jsonize("id", gatherId, gatherId);
            json.Jsonize("prohibit", prohibit, prohibit);
        }
    public:
        std::string patternStr;
        std::string gatherId;
        bool prohibit;
    };
    
public:
    struct Status
    {
    public:
        Status(std::string _gatherId = "", bool _prohibit = false)
            : gatherId(_gatherId)
            , prohibit(_prohibit)
        {}
        
    public:
        std::string gatherId;
        bool prohibit;
    };
    
public:
    IndexlibMetricControl();
    ~IndexlibMetricControl();
    
public:
    void InitFromString(const std::string& paramStr);
    Status Get(const std::string& metricName) const;

public:
    void TEST_RESET()
    { mStatusItems.clear(); }
    
private:
    void InitFromEnv();
    void ParsePatternItem(std::vector<PatternItem> &patternItems, const std::string& patternStr);
    void ParseFlatString(std::vector<PatternItem> &patternItems, const std::string& patternStr);
    void ParseJsonString(std::vector<PatternItem> &patternItems, const std::string& patternStr);

    bool IsExist(const std::string& filePath);
    void AtomicLoad(const std::string& filePath, std::string& fileContent);
    
private:
    typedef std::pair<misc::RegularExpressionPtr, Status> StatusItem;
    typedef std::vector<StatusItem> StatusItemVector;

    StatusItemVector mStatusItems;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexlibMetricControl);

IE_NAMESPACE_END(misc);

#endif //__INDEXLIB_INDEXLIB_METRIC_CONTROL_H
