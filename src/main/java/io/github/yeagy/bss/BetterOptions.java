package io.github.yeagy.bss;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * configuration for BSS classes. in a single DS application i would just set the defaults and use that.
 * right now the only option is array support, geared towards postgres.
 */
public final class BetterOptions {
    public enum Option {ARRAY_SUPPORT}

    private static BetterOptions defaults = new BetterOptions(Collections.emptySet());

    private final Set<Option> options;

    private BetterOptions(Set<Option> options){
        this.options = options;
    }

    public static BetterOptions from(Set<Option> options){
        return new BetterOptions(options);
    }

    public static BetterOptions from(Option... options){
        return from(new HashSet<>(Arrays.asList(options)));
    }

    public static BetterOptions fromDefaults(){
        return defaults;
    }

    public static void setDefaults(Set<Option> options){
        defaults = from(options);
    }

    public static void setDefaults(Option... options){
        defaults = from(options);
    }

    public boolean enabled(Option option){
        return options.contains(option);
    }

    boolean arraySupport(){
        return enabled(Option.ARRAY_SUPPORT);
    }

}
