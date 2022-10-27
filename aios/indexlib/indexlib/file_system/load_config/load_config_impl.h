#ifndef __INDEXLIB_LOAD_CONFIG_IMPL_H
#define __INDEXLIB_LOAD_CONFIG_IMPL_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include <autil/legacy/jsonizable.h>
#include <tr1/memory>

IE_NAMESPACE_BEGIN(file_system);

class LoadConfigImpl : public autil::legacy::Jsonizable
{
public:
    LoadConfigImpl();
    ~LoadConfigImpl();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;
    bool EqualWith(const LoadConfigImpl& other) const;

public:
    void SetLifecycle(const std::string lifecycle) { mLifecycle = lifecycle; }
    const std::string& GetLifecycle() const { return mLifecycle; }

private:
    std::string mLifecycle;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LoadConfigImpl);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_LOAD_CONFIG_IMPL_H
