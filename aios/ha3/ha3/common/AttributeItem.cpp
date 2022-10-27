#include <ha3/common/AttributeItem.h>

using namespace std;
using namespace std::tr1;
using namespace suez::turing;

USE_HA3_NAMESPACE(util);
BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, AttributeItem);

AttributeItem* AttributeItem::createAttributeItem(VariableType variableType,
        bool isMulti)
{
    AttributeItem *ret = NULL;
#define CREATE_ATTRIBUTE_ITEM_CASE_HELPER(vt_type)                      \
    case vt_type:                                                       \
        if (isMulti) {                                                  \
            ret = new AttributeItemTyped<VariableTypeTraits<vt_type, true>::AttrItemType>; \
        } else {                                                        \
            ret = new AttributeItemTyped<VariableTypeTraits<vt_type, false>::AttrItemType>; \
        }                                                               \
        break

    switch(variableType) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER(CREATE_ATTRIBUTE_ITEM_CASE_HELPER);
        CREATE_ATTRIBUTE_ITEM_CASE_HELPER(vt_string);
    default:
        break;
    }
    return ret;
}

END_HA3_NAMESPACE(common);
