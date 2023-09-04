/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <algorithm>
#include <cstdint>
#include <map>
#include <stddef.h>
#include <string>
#include <vector>

namespace autil {

// Note! this is not multi-thread safe!
class OptionParser {
public:
    enum OptionType {
        OPT_STRING = 0,
        OPT_INT32,
        OPT_UINT32,
        OPT_BOOL,
        OPT_HELP,
    };
    enum OptionAction {
        STORE,
        STORE_TRUE,
        STORE_FALSE,
    };

public:
    typedef std::map<std::string, std::string> StrOptMap;
    typedef std::map<std::string, bool> BoolOptMap;
    typedef std::map<std::string, int32_t> IntOptMap;
    typedef std::map<std::string, std::vector<std::string>> MultiValueStrOptMap;

public:
    OptionParser(const std::string &usageDescription = "");
    ~OptionParser();

    void addOption(const std::string &shortOpt,
                   const std::string &longOpt,
                   const std::string &optName,
                   const OptionType type = OPT_STRING,
                   bool isRequired = false);

    void addOption(const std::string &shortOpt,
                   const std::string &longOpt,
                   const std::string &optName,
                   const OptionAction &action,
                   const OptionType type = OPT_BOOL,
                   bool isRequired = false);

    // the following functions are for optional options
    void addOption(const std::string &shortOpt,
                   const std::string &longOpt,
                   const std::string &optName,
                   const char *defaultValue);

    void addOption(const std::string &shortOpt,
                   const std::string &longOpt,
                   const std::string &optName,
                   const std::string &defaultValue);

    void addOption(const std::string &shortOpt,
                   const std::string &longOpt,
                   const std::string &optName,
                   const uint32_t defaultValue);

    void addOption(const std::string &shortOpt,
                   const std::string &longOpt,
                   const std::string &optName,
                   const int32_t defaultValue);

    void addOption(const std::string &shortOpt,
                   const std::string &longOpt,
                   const std::string &optName,
                   const bool defaultValue);
    void addMultiValueOption(const std::string &shortOpt, const std::string &longOpt, const std::string &optName);

    bool parseArgs(int argc, char **argv);
    bool parseArgs(const std::string &commandString);
    bool getOptionValue(const std::string &optName, std::string &value) const;
    bool getOptionValue(const std::string &optName, bool &value) const;
    bool getOptionValue(const std::string &optName, int32_t &value) const;
    bool getOptionValue(const std::string &optName, uint32_t &value) const;
    bool getOptionValue(const std::string &optName, std::vector<std::string> &valueVec) const;

    StrOptMap getOptionValues() const;
    void updateUsageDescription(const std::string &usageDescription);

private:
    class OptionInfo {
    public:
        OptionInfo(const std::string &optionName,
                   OptionType type,
                   OptionAction action,
                   bool required,
                   const std::string &defaultValue = "",
                   bool multiValue = false)
            : optionType(type)
            , optionAction(action)
            , optionName(optionName)
            , isRequired(required)
            , isMultiValue(multiValue)

        {
            isSet = false;
        }
        ~OptionInfo() {}
        bool hasValue() { return optionAction == STORE; }

    public:
        OptionType optionType;
        OptionAction optionAction;
        std::string optionName;
        bool isRequired;
        bool isMultiValue;
        bool isSet;
    };

private:
    void addOptionInfo(const OptionInfo &optionInfo, const std::string &shortOpt, const std::string &longOpt);

    // results:
private:
    StrOptMap _strOptMap;
    IntOptMap _intOptMap;
    BoolOptMap _boolOptMap;
    MultiValueStrOptMap _multiValueStrOptMap;
    std::string _usageDescription;

    // infos:
private:
    typedef std::map<std::string, size_t> ShortOpt2InfoMap;
    typedef std::map<std::string, size_t> LongOpt2InfoMap;
    typedef std::vector<OptionInfo> OptionInfos;
    ShortOpt2InfoMap _shortOpt2InfoMap;
    LongOpt2InfoMap _longOpt2InfoMap;
    OptionInfos _optionInfos;

private:
    void init();
    bool isValidArgs();

private:
    friend class OptionParserTest;
};

} // namespace autil
