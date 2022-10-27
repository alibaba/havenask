#include <ha3/sql/ops/util/KernelUtil.h>
#include <autil/legacy/RapidJsonCommon.h>
#include <ha3/sql/resource/SqlQueryResource.h>

using namespace std;
using namespace navi;

BEGIN_HA3_NAMESPACE(sql);

HA3_LOG_SETUP(sql, KernelUtil);
const std::string KernelUtil::FIELD_PREIFX = "$";

autil::mem_pool::Pool *KernelUtil::getPool(KernelInitContext &context) {
    auto queryResource = context.getResource<SqlQueryResource>("SqlQueryResource");
    if (!queryResource) {
        SQL_LOG(WARN, "invalid query resource");
        return nullptr;
    }
    return queryResource->getPool();
}

void KernelUtil::stripName(std::string &name) {
    if (autil::StringUtil::startsWith(name, FIELD_PREIFX)) {
        name = name.substr(1);
    }
}

void KernelUtil::stripName(std::vector<std::string> &vec) {
    for (auto &v : vec) {
        stripName(v);
    }
}

bool KernelUtil::toJsonString(autil::legacy::Jsonizable::JsonWrapper &wrapper,
                              const string &fieldName, string &outVal)
{
    try {
        if (wrapper.GetFastMagicNum() == FAST_MODE_MAGIC_NUMBER) {
            auto *val = wrapper.GetRapidValue();
            if (val == nullptr) {
                return false;
            }
            if (!val->IsObject()) {
                return false;
            }
            if (!val->HasMember(fieldName)) {
                return true;
            }
            const auto &rVal = (*val)[fieldName];
            autil_rapidjson::StringBuffer buf;
            autil_rapidjson::Writer<autil_rapidjson::StringBuffer> jWriter(buf);
            rVal.Accept(jWriter);
            outVal = buf.GetString();
        } else {
            autil::legacy::Any anyVal;
            wrapper.Jsonize(fieldName, anyVal, anyVal);
            if (!anyVal.IsEmpty()) {
                autil::legacy::json::ToString(anyVal, outVal, true);
            }
        }
    } catch (const autil::legacy::ExceptionBase &e) {
        SQL_LOG(ERROR, "parse json fail, field name [%s].", fieldName.c_str());
        return false;
    } catch (...) {
        return false;
    }
    return true;
}

bool KernelUtil::toJsonWrapper(
        autil::legacy::Jsonizable::JsonWrapper &wrapper,
        const std::string &fieldName,
        autil::legacy::Jsonizable::JsonWrapper &outWrapper)
{

    if (wrapper.GetFastMagicNum() == FAST_MODE_MAGIC_NUMBER) {
        auto *val = wrapper.GetRapidValue();
        if (val == nullptr) {
            return false;
        }
        if (!val->IsObject()) {
            return false;
        }
        if (!val->HasMember(fieldName)) {
            return false;
        }
        outWrapper = autil::legacy::Jsonizable::JsonWrapper(&(*val)[fieldName]);
    } else {
        auto jsonMap = wrapper.GetMap();
        if (jsonMap.find(fieldName) == jsonMap.end()) {
            return false;
        }
        auto &anyNode = jsonMap[fieldName];
        outWrapper = autil::legacy::Jsonizable::JsonWrapper(anyNode);
    }
    return true;
}

END_HA3_NAMESPACE(sql);
