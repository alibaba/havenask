#pragma once

#include "indexlib/plugin/ModuleFactory.h"

namespace indexlib { namespace plugin {
class DummyObj
{
public:
    virtual ~DummyObj() {}

public:
    virtual std::string getName() = 0;
};

class DummyObjFactory : public plugin::ModuleFactory
{
public:
    virtual ~DummyObjFactory() {}
    virtual DummyObj* createObj(const std::string& name) = 0;
};

}} // namespace indexlib::plugin
