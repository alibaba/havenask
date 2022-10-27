#include "indexlib/config/impl/summary_config_impl.h"
#include "autil/legacy/any.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/json.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, SummaryConfigImpl);

void SummaryConfigImpl::AssertEqual(const SummaryConfigImpl& other) const
{
    if (mFieldConfig != NULL && other.mFieldConfig != NULL)
    {
        mFieldConfig->AssertEqual(*(other.mFieldConfig));
    }
    else if (mFieldConfig != NULL || other.mFieldConfig != NULL)
    {
        INDEXLIB_FATAL_ERROR(AssertEqual, "mFieldConfig is not equal");
    }
}

IE_NAMESPACE_END(config);

