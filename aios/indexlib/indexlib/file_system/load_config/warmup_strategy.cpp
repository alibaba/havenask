#include "indexlib/file_system/load_config/warmup_strategy.h"
#include "indexlib/misc/exception.h"

using namespace std;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, WarmupStrategy);

static const string WARMUP_NONE_TYPE_STRING = string("none");
static const string WARMUP_SEQUENTIAL_TYPE_STRING = string("sequential");

WarmupStrategy::WarmupStrategy() 
    : mWarmupType(WARMUP_NONE)
{
}

WarmupStrategy::~WarmupStrategy() 
{
}

WarmupStrategy::WarmupType WarmupStrategy::FromTypeString(
        const string& typeStr)
{
    if (typeStr == WARMUP_NONE_TYPE_STRING)
    {
        return WarmupStrategy::WARMUP_NONE;
    }
    if (typeStr == WARMUP_SEQUENTIAL_TYPE_STRING)
    {
        return WarmupStrategy::WARMUP_SEQUENTIAL;
    }
    INDEXLIB_THROW(misc::BadParameterException, "unsupported warmup strategy [ %s ]",
                   typeStr.c_str());
    return WarmupStrategy::WARMUP_NONE;
}

string WarmupStrategy::ToTypeString(const WarmupType& type)
{
    if (type == WARMUP_NONE)
    {
        return WARMUP_NONE_TYPE_STRING;
    }
    if (type == WARMUP_SEQUENTIAL)
    {
        return WARMUP_SEQUENTIAL_TYPE_STRING;
    }
    INDEXLIB_THROW(misc::BadParameterException, "unsupported enum warmup type [ %d ]", type);
    return WARMUP_NONE_TYPE_STRING;
}

IE_NAMESPACE_END(file_system);

