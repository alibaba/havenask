#ifndef __INDEXLIB_ONLINE_CONFIG_IMPL_H
#define __INDEXLIB_ONLINE_CONFIG_IMPL_H

#include <tr1/memory>
#include <autil/legacy/jsonizable.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/disable_fields_config.h"

IE_NAMESPACE_BEGIN(config);

class OnlineConfigImpl : public autil::legacy::Jsonizable
{
public:
    OnlineConfigImpl();
    OnlineConfigImpl(const OnlineConfigImpl& other);
    ~OnlineConfigImpl();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;
    bool operator == (const OnlineConfigImpl& other) const;
    void operator = (const OnlineConfigImpl& other);

public:
    void SetEnableRedoSpeedup(bool enableFlag);
    bool IsRedoSpeedupEnabled() const; 
    bool IsValidateIndexEnabled() const;
    bool IsReopenRetryOnIOExceptionEnabled() const;
    const DisableFieldsConfig& GetDisableFieldsConfig() const;
    DisableFieldsConfig& GetDisableFieldsConfig();
    void SetInitReaderThreadCount(int32_t initReaderThreadCount);
    int32_t GetInitReaderThreadCount() const;
    void SetVersionTsAlignment(int64_t ts); 
    int64_t GetVersionTsAlignment() const;

private:
    bool mEnableRedoSpeedup;
    bool mEnableValidateIndex;
    bool mEnableReopenRetryOnIOException;
    DisableFieldsConfig mDisableFieldsConfig;
    int32_t mInitReaderThreadCount;
    int64_t mVersionTsAlignment; // in microseconds
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OnlineConfigImpl);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_ONLINE_CONFIG_IMPL_H
