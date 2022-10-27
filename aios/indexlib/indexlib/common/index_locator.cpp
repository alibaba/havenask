#include "indexlib/common/index_locator.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(common);

const IndexLocator INVALID_INDEX_LOCATOR;

string IndexLocator::toString() const
{
    string result;
    result.resize(size());
    char *data = (char *)result.data();
    *(uint64_t *)data = _src;
    data += sizeof(_src);
    *(int64_t *)data = _offset;
    return result;
}

string IndexLocator::toDebugString() const
{ return StringUtil::toString(_src) + ":" + StringUtil::toString(_offset); }

IE_NAMESPACE_END(common);
