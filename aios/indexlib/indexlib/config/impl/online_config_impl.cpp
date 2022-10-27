#include <autil/StringUtil.h>
#include "indexlib/config/impl/online_config_impl.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil::legacy;
using namespace autil;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, OnlineConfigImpl);

OnlineConfigImpl::OnlineConfigImpl()
    : mEnableRedoSpeedup(false)
    , mEnableValidateIndex(true)
    , mEnableReopenRetryOnIOException(true) 
    , mInitReaderThreadCount(0)
    , mVersionTsAlignment(1000000) 
{}

OnlineConfigImpl::OnlineConfigImpl(const OnlineConfigImpl& other)
    : mEnableRedoSpeedup(other.mEnableRedoSpeedup)
    , mEnableValidateIndex(other.mEnableValidateIndex)
    , mEnableReopenRetryOnIOException(other.mEnableReopenRetryOnIOException)
    , mDisableFieldsConfig(other.mDisableFieldsConfig)
    , mInitReaderThreadCount(other.mInitReaderThreadCount)
    , mVersionTsAlignment(other.mVersionTsAlignment)
{}

OnlineConfigImpl::~OnlineConfigImpl() {}

void OnlineConfigImpl::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize("enable_redo_speedup", mEnableRedoSpeedup, mEnableRedoSpeedup); 
    json.Jsonize("enable_validate_index", mEnableValidateIndex, mEnableValidateIndex); 
    json.Jsonize("enable_reopen_retry_io_exception", mEnableReopenRetryOnIOException, mEnableReopenRetryOnIOException); 
    json.Jsonize("disable_fields", mDisableFieldsConfig, mDisableFieldsConfig);
    json.Jsonize("init_reader_thread_count", mInitReaderThreadCount, mInitReaderThreadCount);
    json.Jsonize("version_timestamp_alignment", mVersionTsAlignment, mVersionTsAlignment); 
}

void OnlineConfigImpl::Check() const
{
}

bool OnlineConfigImpl::operator == (const OnlineConfigImpl& other) const
{
    return mEnableRedoSpeedup == other.mEnableRedoSpeedup
        && mEnableValidateIndex == other.mEnableValidateIndex
        && mEnableReopenRetryOnIOException == other.mEnableReopenRetryOnIOException
        && mDisableFieldsConfig == other.mDisableFieldsConfig
        && mInitReaderThreadCount == other.mInitReaderThreadCount
        && mVersionTsAlignment == other.mVersionTsAlignment;
}

void OnlineConfigImpl::operator = (const OnlineConfigImpl& other)
{
    mEnableValidateIndex = other.mEnableValidateIndex;
    mEnableReopenRetryOnIOException = other.mEnableReopenRetryOnIOException;
    mDisableFieldsConfig = other.mDisableFieldsConfig;
    mEnableRedoSpeedup = other.mEnableRedoSpeedup;
    mInitReaderThreadCount = other.mInitReaderThreadCount;
    mVersionTsAlignment = other.mVersionTsAlignment;
}
 
void OnlineConfigImpl::SetEnableRedoSpeedup(bool enableFlag)
{
    mEnableRedoSpeedup = enableFlag;
}

bool OnlineConfigImpl::IsRedoSpeedupEnabled() const
{
    return mEnableRedoSpeedup;
}

bool OnlineConfigImpl::IsValidateIndexEnabled() const
{
    return mEnableValidateIndex;
}

bool OnlineConfigImpl::IsReopenRetryOnIOExceptionEnabled() const
{
    return mEnableReopenRetryOnIOException;
}

const DisableFieldsConfig& OnlineConfigImpl::GetDisableFieldsConfig() const
{
    return mDisableFieldsConfig;
}

DisableFieldsConfig& OnlineConfigImpl::GetDisableFieldsConfig()
{
    return mDisableFieldsConfig;
}

void OnlineConfigImpl::SetInitReaderThreadCount(int32_t initReaderThreadCount)
{
    mInitReaderThreadCount = initReaderThreadCount;
}

int32_t OnlineConfigImpl::GetInitReaderThreadCount() const
{
    return mInitReaderThreadCount;
}

void OnlineConfigImpl::SetVersionTsAlignment(int64_t ts)
{
    mVersionTsAlignment = ts;
}

int64_t OnlineConfigImpl::GetVersionTsAlignment() const
{
    return mVersionTsAlignment;
}

IE_NAMESPACE_END(config);

