package com.taobao.search.iquan.core.api.config;

import java.util.Objects;

public class IquanConfiguration<T> {
    private String key;
    private T defaultValue;
    private Class<?> clazz;
    private String description;

    public IquanConfiguration(String key, T defaultValue, Class<?> clazz, String description) {
        this.key = key;
        this.defaultValue = defaultValue;
        this.clazz = clazz;
        this.description = description;
    }

    private IquanConfiguration() {

    }

    public static <K> IquanConfigBuilder<K> builder() {
        return new IquanConfigBuilder<K>();
    }

    public String key() {
        return this.key;
    }

    public boolean hasDefaultValue() {
        return this.defaultValue != null;
    }

    public T defaultValue() {
        return this.defaultValue;
    }

    Class<?> getClazz() {
        return this.clazz;
    }

    public String getDescription() {
        return description;
    }

    public boolean isValid() {
        return key != null && clazz != null;
    }

    public int hashCode() {
        return 31 * this.key.hashCode() + (this.defaultValue != null ? this.defaultValue.hashCode() : 0);
    }

    public String toString() {
        return String.format("Key: '%s' , default: %s ", this.key, this.defaultValue);
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o != null && o.getClass() == IquanConfiguration.class) {
            IquanConfiguration<?> that = (IquanConfiguration) o;
            return Objects.equals(this.key, that.key) && Objects.equals(this.defaultValue, that.defaultValue);
        }
        return false;
    }

    public static class IquanConfigBuilder<T> {
        private IquanConfiguration<T> configuration = new IquanConfiguration<>();

        public IquanConfigBuilder<T> key(String key) {
            configuration.key = key;
            return this;
        }

        public IquanConfigBuilder<T> defaultValue(T value) {
            configuration.defaultValue = value;
            return this;
        }

        public IquanConfigBuilder<T> clazz(Class<?> clazz) {
            configuration.clazz = clazz;
            return this;
        }

        public IquanConfigBuilder<T> description(String description) {
            configuration.description = description;
            return this;
        }

        private boolean isValid() {
            return configuration.isValid();
        }

        public IquanConfiguration<T> build() {
            if (!isValid()) {
                throw new RuntimeException("sqlConfiguration is invalid");
            }
            return configuration;
        }
    }
}
