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
#include "autil/OptionParser.h"

#include <iostream>
#include <limits>
#include <stdio.h>
#include <utility>

#include "autil/CommandLineParameter.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"

using namespace std;
namespace autil {
AUTIL_DECLARE_AND_SETUP_LOGGER(autil, OptionParser);

OptionParser::OptionParser(const string &usageDescription) { _usageDescription = usageDescription; }

OptionParser::~OptionParser() {}

void OptionParser::addOptionInfo(const OptionInfo &optionInfo, const string &shortOpt, const string &longOpt) {
    size_t index = _optionInfos.size();
    if (!shortOpt.empty()) {
        _shortOpt2InfoMap.insert(make_pair(shortOpt, index));
    }
    if (!longOpt.empty()) {
        _longOpt2InfoMap.insert(make_pair(longOpt, index));
    }
    _optionInfos.push_back(optionInfo);
}

void OptionParser::addOption(const string &shortOpt,
                             const string &longOpt,
                             const string &optName,
                             const char *defaultValue) {
    addOption(shortOpt, longOpt, optName, string(defaultValue));
}

void OptionParser::addOption(const string &shortOpt,
                             const string &longOpt,
                             const string &optName,
                             const string &defaultValue) {
    OptionInfo optionInfo(optName, OPT_STRING, STORE, false);
    addOptionInfo(optionInfo, shortOpt, longOpt);
    _strOptMap[optName] = defaultValue;
}

void OptionParser::addOption(const string &shortOpt,
                             const string &longOpt,
                             const string &optName,
                             const uint32_t defaultValue) {
    string defaultStringValue = StringUtil::toString(defaultValue);
    OptionInfo optionInfo(optName, OPT_UINT32, STORE, false);
    addOptionInfo(optionInfo, shortOpt, longOpt);
    _intOptMap[optName] = (int32_t)defaultValue;
}

void OptionParser::addOption(const string &shortOpt,
                             const string &longOpt,
                             const string &optName,
                             const int32_t defaultValue) {
    string defaultStringValue = StringUtil::toString(defaultValue);
    OptionInfo optionInfo(optName, OPT_INT32, STORE, false);
    addOptionInfo(optionInfo, shortOpt, longOpt);
    _intOptMap[optName] = defaultValue;
}

void OptionParser::addOption(const string &shortOpt,
                             const string &longOpt,
                             const string &optName,
                             const bool defaultValue) {
    string defaultStringValue = StringUtil::toString(defaultValue);
    OptionInfo optionInfo(optName, OPT_BOOL, STORE_TRUE, false);
    addOptionInfo(optionInfo, shortOpt, longOpt);
    _boolOptMap[optName] = defaultValue;
}

void OptionParser::addOption(
    const string &shortOpt, const string &longOpt, const string &optName, const OptionType type, bool isRequired) {
    OptionInfo optionInfo(optName, type, STORE, isRequired);
    addOptionInfo(optionInfo, shortOpt, longOpt);
}

void OptionParser::addOption(const string &shortOpt,
                             const string &longOpt,
                             const string &optName,
                             const OptionAction &action,
                             const OptionType type,
                             bool isRequired) {
    OptionInfo optionInfo(optName, type, action, isRequired);
    addOptionInfo(optionInfo, shortOpt, longOpt);
}

void OptionParser::addMultiValueOption(const string &shortOpt, const string &longOpt, const string &optName) {
    OptionInfo optionInfo(optName, OPT_STRING, STORE, false, "", true);
    addOptionInfo(optionInfo, shortOpt, longOpt);
}

bool OptionParser::parseArgs(int argc, char **argv) {
    string option;
    for (int i = 0; i < argc; ++i) {
        string optName = string(argv[i]);
        size_t index;
        ShortOpt2InfoMap::const_iterator shortIt = _shortOpt2InfoMap.find(optName);
        if (shortIt == _shortOpt2InfoMap.end()) {
            LongOpt2InfoMap::const_iterator longIt = _longOpt2InfoMap.find(optName);
            if (longIt == _longOpt2InfoMap.end()) {
                continue;
            } else {
                index = longIt->second;
            }
        } else {
            index = shortIt->second;
        }
        OptionInfo &info = _optionInfos[index];
        if (info.optionType == OPT_HELP) {
            cout << _usageDescription << endl;
            return false;
        }

        string optarg;
        if (info.hasValue()) {

            if (i + 1 >= argc) {
                fprintf(stderr, "Option parse error: option [%s] must have value!\n", optName.c_str());
                return false;
            } else {
                ShortOpt2InfoMap::const_iterator shortIt = _shortOpt2InfoMap.find(argv[i + 1]);
                LongOpt2InfoMap::const_iterator longIt = _longOpt2InfoMap.find(argv[i + 1]);
                if (shortIt != _shortOpt2InfoMap.end() || (longIt != _longOpt2InfoMap.end())) {
                    fprintf(stderr, "Option parse error: option [%s] must have value!\n", optName.c_str());
                    return false;
                }
                optarg = argv[++i];
            }
        }

        info.isSet = true;
        switch (info.optionType) {
        case OPT_STRING:
            if (!info.isMultiValue) {
                _strOptMap[info.optionName] = optarg;
            } else {
                _multiValueStrOptMap[info.optionName].push_back(optarg);
            }
            break;
        case OPT_INT32: {
            int32_t intValue;
            if (StringUtil::strToInt32(optarg.c_str(), intValue) == false) {
                fprintf(stderr,
                        "Option parse error: invalid int32 value[%s] for option [%s]\n",
                        optarg.c_str(),
                        optName.c_str());
                return false;
            }
            _intOptMap[info.optionName] = intValue;
            break;
        }
        case OPT_UINT32: {
            int64_t intValue;
            if (StringUtil::strToInt64(optarg.c_str(), intValue) == false ||
                intValue < numeric_limits<uint32_t>::min() || intValue > numeric_limits<uint32_t>::max()) {
                fprintf(stderr,
                        "Option parse error: invalid uint32 value[%s] for option [%s]\n",
                        optarg.c_str(),
                        optName.c_str());
                return false;
            }
            _intOptMap[info.optionName] = (int32_t)intValue;
            break;
        }
        case OPT_BOOL:
            _boolOptMap[info.optionName] = info.optionAction == STORE_TRUE ? true : false;
            break;
        default:
            continue;
        }
    }

    if (!isValidArgs()) {
        return false;
    }
    return true;
}

bool OptionParser::isValidArgs() {
    for (OptionInfos::const_iterator it = _optionInfos.begin(); it != _optionInfos.end(); ++it) {
        OptionInfo info = (*it);
        if (info.isRequired && info.isSet == false) {
            fprintf(stderr, "Option parse error: missing required option [%s]\n", info.optionName.c_str());
            return false;
        }
    }

    return true;
}

bool OptionParser::parseArgs(const string &commandString) {
    CommandLineParameter cmdLinePara(commandString);
    return parseArgs(cmdLinePara.getArgc(), cmdLinePara.getArgv());
}

OptionParser::StrOptMap OptionParser::getOptionValues() const { return _strOptMap; }

bool OptionParser::getOptionValue(const string &optName, string &value) const {
    StrOptMap::const_iterator it = _strOptMap.find(optName);
    if (it != _strOptMap.end()) {
        value = it->second;
        return true;
    }
    return false;
}

bool OptionParser::getOptionValue(const string &optName, bool &value) const {
    BoolOptMap::const_iterator it = _boolOptMap.find(optName);
    if (it != _boolOptMap.end()) {
        value = it->second;
        return true;
    }
    return false;
}

bool OptionParser::getOptionValue(const string &optName, int32_t &value) const {
    IntOptMap::const_iterator it = _intOptMap.find(optName);
    if (it != _intOptMap.end()) {
        value = it->second;
        return true;
    }
    return false;
}

bool OptionParser::getOptionValue(const string &optName, uint32_t &value) const {
    IntOptMap::const_iterator it = _intOptMap.find(optName);
    if (it != _intOptMap.end()) {
        value = (uint32_t)it->second;
        return true;
    }
    return false;
}

bool OptionParser::getOptionValue(const string &optName, vector<string> &valueVec) const {
    MultiValueStrOptMap::const_iterator it = _multiValueStrOptMap.find(optName);
    if (it != _multiValueStrOptMap.end()) {
        valueVec.clear();
        valueVec.insert(valueVec.end(), it->second.begin(), it->second.end());
        return true;
    }
    return false;
}

void OptionParser::updateUsageDescription(const string &usageDescription) { _usageDescription = usageDescription; }

} // namespace autil
