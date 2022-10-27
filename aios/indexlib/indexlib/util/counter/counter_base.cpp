#include "indexlib/util/counter/counter_base.h"

using namespace std;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(util);

const std::string CounterBase::TYPE_META = "__type__";

CounterBase::CounterBase(const string& path, CounterType type)
    : mPath(path)
    , mType(type)
{
}

CounterBase::~CounterBase() 
{
}

IE_NAMESPACE_END(util);

