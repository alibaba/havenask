#pragma once

#include "indexlib/common_define.h"
#include "indexlib/plugin/ModuleFactory.h"

namespace indexlib { namespace plugin {

class NamedFactoryModule : public ModuleFactory
{
public:
    NamedFactoryModule() {}
    ~NamedFactoryModule() {}

public:
    virtual const std::string& getSuffix() const = 0;
};

DEFINE_SHARED_PTR(NamedFactoryModule);

}} // namespace indexlib::plugin
