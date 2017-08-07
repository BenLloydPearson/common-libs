package com.gravity.utilities;/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GravityRoleProperties {
    Properties allGravityProps;

    public GravityRoleProperties(Properties allGravityProps) {
        this.allGravityProps = allGravityProps;
    }

    public Object setProperty(String key, String value) {
        return allGravityProps.setProperty(key, value);
    }

    public String getProperty(String key) {
        return getProperty(key, null);
    }

    public String getProperty(String key, String defaultValue) {
        String sysOverride = System.getProperty(key);
        if (sysOverride != null) {
            return sysOverride;
        }

        String override = allGravityProps.getProperty(key + "." + System.getProperty("user.name"));
        if (override != null) {
            return override;
        }

        String role = Settings.APPLICATION_ROLE;
        override = allGravityProps.getProperty(key + "." + role);
        if (override != null) {
            return override;
        }


        String environmentOverride = allGravityProps.getProperty(key + "." + Settings.ENVIRONMENT);
        if (environmentOverride != null) {
            return environmentOverride;
        }


        return allGravityProps.getProperty(key, defaultValue);
    }

    public Set<String> getPropertyNames() {
        return Sets.filter(allGravityProps.stringPropertyNames(), new Predicate<String>() {
            public boolean apply(String propName) {
                return !propName.startsWith("application.role.type");
            }
        });
    }

    private static Pattern roleOverridePropsPattern() {
        return Pattern.compile(String.format("(.*)\\.(%s)$", Settings.APPLICATION_ROLE));
    }

    void validateApplicationRolePropertyOverrides() {
        Pattern roleOverridePropsPattern = roleOverridePropsPattern();

        Map<String, Set<String>> basePropNameToRoleValues = new HashMap<String, Set<String>>();
        for (String prop : getPropertyNames()) {
            Matcher m = roleOverridePropsPattern.matcher(prop);
            if (!m.find()) {
                continue;
            }

            String basePropName = m.group(1);
            String value = allGravityProps.getProperty(prop);

            Set<String> values = basePropNameToRoleValues.get(basePropName);
            if (values == null) {
                values = new HashSet<String>();
            }

            values.add(value);
            basePropNameToRoleValues.put(basePropName, values);
        }

        for (Map.Entry<String, Set<String>> roleValues : basePropNameToRoleValues.entrySet()) {
            if (roleValues.getValue().size() > 1) {
                throw new IllegalArgumentException(String.format(
                        "Property %s has conflicting overrides for roles [%s]: %s",
                        roleValues.getKey(),
                        Settings.APPLICATION_ROLE,
                        StringUtils.join(roleValues.getValue(), ", ")
                ));
            }
        }
    }
}
