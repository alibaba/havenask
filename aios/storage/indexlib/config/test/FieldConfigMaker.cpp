#include "indexlib/config/test/FieldConfigMaker.h"

#include "autil/StringTokenizer.h"

using namespace std;

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.config, FieldConfigMaker);

vector<shared_ptr<FieldConfig>> FieldConfigMaker::Make(const std::string& fieldNamesStr)
{
    // fieldName:fieldType:isMultiValue:FixedMultiValueCount:EnableNullField:AnalyzerName
    auto fieldNames = SplitToStringVector(fieldNamesStr);
    vector<shared_ptr<FieldConfig>> fieldConfigs;
    for (size_t i = 0; i < fieldNames.size(); ++i) {
        autil::StringTokenizer st(fieldNames[i], ":", autil::StringTokenizer::TOKEN_TRIM);

        assert(st.getNumTokens() >= 2 && st.getNumTokens() <= 7);

        bool isMultiValue = false;
        if (st.getNumTokens() >= 3) {
            isMultiValue = (st[2] == "true");
        }
        FieldType fieldType = ft_unknown;
        [[maybe_unused]] auto ret = FieldConfig::StrToFieldType(st[1], fieldType);
        assert(ret);
        auto fieldConfig = make_shared<FieldConfig>(st[0], fieldType, isMultiValue);
        fieldConfig->SetFieldId(i);

        if (st.getNumTokens() >= 4) {
            int32_t fixedMultiValueCount = -1;
            [[maybe_unused]] bool ret = autil::StringUtil::fromString(st[3], fixedMultiValueCount);
            assert(ret);
            assert(fixedMultiValueCount != 0);
            fieldConfig->SetFixedMultiValueCount(fixedMultiValueCount);
        }

        if (st.getNumTokens() >= 5 && st[4] == "true") {
            fieldConfig->SetEnableNullField(true);
        }

        if (fieldType == ft_text) {
            fieldConfig->SetAnalyzerName("taobao_analyzer");
        }
        fieldConfigs.push_back(fieldConfig);
    }
    return fieldConfigs;
}

vector<string> FieldConfigMaker::SplitToStringVector(const string& names)
{
    vector<string> sv;
    autil::StringTokenizer st(names, ";",
                              autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
    for (autil::StringTokenizer::Iterator it = st.begin(); it != st.end(); ++it) {
        sv.push_back(*it);
    }
    return sv;
}

} // namespace indexlibv2::config
