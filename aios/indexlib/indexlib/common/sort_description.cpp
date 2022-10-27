#include "indexlib/common/sort_description.h"
#include "indexlib/config/sort_pattern_transformer.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(common);

void SortDescription::Jsonize(JsonWrapper &json)
{
    json.Jsonize("SortField", sortFieldName, sortFieldName);
    if (json.GetMode() == TO_JSON)
    {
        string sortPatternStr =
            SortPatternTransformer::ToString(sortPattern);
        json.Jsonize("SortPattern", sortPatternStr);
    }
    else
    {
        // for compatible
        map<string, Any> jsonMap = json.GetMap();
        if (jsonMap.find("sort_field") != jsonMap.end())
        {
            json.Jsonize("sort_field", sortFieldName);
        } 

        // sort pattern
        string sortPatternStr;
        json.Jsonize("SortPattern", sortPatternStr,
                     sortPatternStr);
        if (jsonMap.find("sort_pattern") != jsonMap.end())
        {
            json.Jsonize("sort_pattern", sortPatternStr);
        }
        sortPattern = SortPatternTransformer::FromString(
                sortPatternStr);
    }
}

bool SortDescription::operator==(const SortDescription &other) const
{
    return sortFieldName == other.sortFieldName 
        && sortPattern == other.sortPattern;
}

string SortDescription::ToString() const
{
    return ToJsonString(*this);
}


IE_NAMESPACE_END(common);

