#pragma once
#include <ha3/sql/ops/tvf/TvfFunc.h>
#include <ha3/config/SqlTvfProfileInfo.h>
#include <ha3/config/ResourceReader.h>
#include <iquan_common/catalog/json/TvfFunctionModel.h>
#include <map>

BEGIN_HA3_NAMESPACE(sql);

class TvfFuncCreator {
public:
    TvfFuncCreator(const std::string &tvfDef,
                   HA3_NS(config)::SqlTvfProfileInfo info = HA3_NS(config)::SqlTvfProfileInfo())
        : _tvfDef(tvfDef)
    {
        if (!info.empty()) {
            addTvfFunction(info);
        }
    }
    virtual ~TvfFuncCreator() {}
public:
    virtual bool init(HA3_NS(config)::ResourceReader *resourceReader) { return true; }
private:
    virtual bool initTvfModel(iquan::TvfModel &tvfModel,
                              const HA3_NS(config)::SqlTvfProfileInfo &info)
    {
        return true;
    }
    virtual TvfFunc *createFunction(const HA3_NS(config)::SqlTvfProfileInfo &info) = 0;

public:
    bool regTvfModels(iquan::TvfModels &tvfModels);
    TvfFunc *createFunction(const std::string &name) {
        auto iter = _sqlTvfProfileInfos.find(name);
        if (iter == _sqlTvfProfileInfos.end()) {
            SQL_LOG(WARN, "tvf [%s]'s func info not found.", name.c_str());
            return nullptr;
        }
        TvfFunc *tvfFunc = createFunction(iter->second);
        if (tvfFunc != nullptr) {
            tvfFunc->setName(name);
        }
        return tvfFunc;
    }
    std::string getName() { return _name; }
    void setName(const std::string &name) { _name = name; }
    void addTvfFunction(const HA3_NS(config)::SqlTvfProfileInfo &sqlTvfProfileInfo) {
        SQL_LOG(INFO, "add tvf function info [%s]", sqlTvfProfileInfo.tvfName.c_str());
        _sqlTvfProfileInfos[sqlTvfProfileInfo.tvfName] = sqlTvfProfileInfo;
    }
    HA3_NS(config)::SqlTvfProfileInfo getDefaultTvfInfo() {
        if (_sqlTvfProfileInfos.size() == 1) {
            return _sqlTvfProfileInfos.begin()->second;
        }
        return {};
    }
private:
    static bool checkTvfModel(const iquan::TvfModel& tvfModel);
    bool generateTvfModel(iquan::TvfModel &tvfModel,
                          const HA3_NS(config)::SqlTvfProfileInfo &info);
protected:
    std::map<std::string, HA3_NS(config)::SqlTvfProfileInfo> _sqlTvfProfileInfos;
    std::string _name;
    std::string _tvfDef;

protected:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(TvfFuncCreator);
typedef std::map<std::string, TvfFuncCreator*> TvfFuncCreatorMap;

END_HA3_NAMESPACE(sql);
