package com.taobao.search.iquan.core.api.config;

import java.util.HashMap;
import java.util.Optional;

public class IquanConfigManager {
    protected final HashMap<String, Object> confData = new HashMap<>();
    
    
// ------------ String ----------------------
    public String getString(String key, String defaultValue) {
        return getRawValue(key)
                .map(IquanConfigUtils::convertToString)
                .orElse(defaultValue);
    }

    public String getString(IquanConfiguration<String> config) {
        Optional<String> result = getOptional(config);
        return result.orElseGet(config::defaultValue);
    }

    public String getString(IquanConfiguration<String> config, String overrideDefault) {
        return getOptional(config).orElse(overrideDefault);
    }

    public void setString(String key, String value) {
        setValueInternal(key, value);
    }
    
    public void setString(IquanConfiguration<String> key, String value) {
        setValueInternal(key.key(), value);
    }

    // ------------ Integer ----------------------

    public int getInteger(String key, int defaultValue) {
        return getRawValue(key).map(IquanConfigUtils::convertToInt).orElse(defaultValue);
    }

    public int getInteger(IquanConfiguration<Integer> config) {
        Optional<Integer> result = getOptional(config);
        return result.orElseGet(config::defaultValue);
    }

    public int getInteger(IquanConfiguration<Integer> config, int overrideDefault) {
        return getOptional(config).orElse(overrideDefault);
    }

    public void setInteger(String key, int value) {
        setValueInternal(key, value);
    }

    public void setInteger(IquanConfiguration<Integer> key, int value) {
        setValueInternal(key.key(), value);
    }

    // ------------ Boolean ----------------------

    public boolean getBoolean(String key, boolean defaultValue) {
        return getRawValue(key).map(IquanConfigUtils::convertToBoolean).orElse(defaultValue);
    }

    public boolean getBoolean(IquanConfiguration<Boolean> config) {
        Optional<Boolean> result = getOptional(config);
        return result.orElseGet(config::defaultValue);
    }

    public boolean getBoolean(IquanConfiguration<Boolean> config, boolean overrideDefault) {
        return getOptional(config).orElse(overrideDefault);
    }

    public void setBoolean(String key, boolean value) {
        setValueInternal(key, value);
    }

    public void setBoolean(IquanConfiguration<Boolean> key, boolean value) {
        setValueInternal(key.key(), value);
    }

    // ------------ Double ----------------------
    public double getDouble(String key, double defaultValue) {
        return getRawValue(key).map(IquanConfigUtils::convertToDouble).orElse(defaultValue);
    }

    public double getDouble(IquanConfiguration<Double> config) {
        Optional<Double> result = getOptional(config);
        return result.orElseGet(config::defaultValue);
    }

    public double getDouble(IquanConfiguration<Double> config, double overrideDefault) {
        return getOptional(config).orElse(overrideDefault);
    }

    public void setDouble(String key, double value) {
        setValueInternal(key, value);
    }

    public void setDouble(IquanConfiguration<Double> key, double value) {
        setValueInternal(key.key(), value);
    }

    // ------------ utils ----------------------

    public <T> Optional<T> getOptional(IquanConfiguration<T> config) {
        Optional<Object> rawValue = getRawValueFromConfig(config);
        Class<?> clazz = config.getClazz();

        try {
            return Optional.ofNullable(IquanConfigUtils.convertValue(rawValue, clazz));
        } catch (Exception var5) {
            throw new IllegalArgumentException(String.format("Could not parse value '%s' for key '%s'.", rawValue.map(Object::toString).orElse(""), config.key()), var5);
        }
    }

    private Optional<Object> getRawValueFromConfig(IquanConfiguration<?> configuration) {
        return getRawValue(configuration.key());
    }

    <T> void setValueInternal(String key, T value) {
        if (key == null) {
            throw new NullPointerException("Key must not be null.");
        } else if (value == null) {
            throw new NullPointerException("Value must not be null.");
        } else {
            synchronized(confData) {
                confData.put(key, value);
            }
        }
    }

    private Optional<Object> getRawValue(String key) {
        if (key == null) {
            throw new NullPointerException("Key must not be null.");
        } else {
            synchronized(confData) {
                return Optional.ofNullable(confData.get(key));
            }
        }
    }

    public <T> IquanConfigManager set(IquanConfiguration<T> option, T value) {
        setValueInternal(option.key(), value);
        return this;
    }
    
    public boolean containsKey(String key) {
        if (key == null) {
            return false;
        }
        synchronized(confData) {
            return confData.containsKey(key);
        }
    }
}
