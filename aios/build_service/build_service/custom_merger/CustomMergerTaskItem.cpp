#include "build_service/custom_merger/CustomMergerTaskItem.h"
#include <autil/MurmurHash.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace autil::legacy;

namespace build_service {
namespace custom_merger {
BS_LOG_SETUP(custom_merger, CustomMergerTaskItem);

void CustomMergerField::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize("field_name", fieldName, fieldName);
    if (json.GetMode() == TO_JSON) {
        string fieldTypeStr;
        if (fieldType == INDEX) {
            fieldTypeStr = "index";
        } else {
            assert(fieldType == ATTRIBUTE);
            fieldTypeStr = "attribute";
        }
        json.Jsonize("field_type", fieldTypeStr, fieldTypeStr);
    } else {
        string type;
        json.Jsonize("field_type", type, type);
        if (type == "index") {
            fieldType = INDEX;
        } else {
            assert(type == "attribute");
            fieldType = ATTRIBUTE;
        }
    }
}

CustomMergerTaskItem::CustomMergerTaskItem() {
}

CustomMergerTaskItem::~CustomMergerTaskItem() {
}

void CustomMergerTaskItem::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize("custom_merge_fields", fields);
    json.Jsonize("cost", cost);
}

uint64_t CustomMergerTaskItem::generateTaskKey() const {
    vector<string> fieldNames;
    fieldNames.reserve(fields.size());
    for (const auto &field : fields) {
        fieldNames.push_back(field.fieldName);
    }
    std::sort(fieldNames.begin(), fieldNames.end());
    string str = StringUtil::toString(fieldNames);
    return MurmurHash::MurmurHash64A(str.c_str(), str.length(), 0);
}

}
}
