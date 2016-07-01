package io.github.yeagy.bss;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class BetterOptions {
    public enum Option {ARRAY_SUPPORT}

    private final Set<Option> options;

    private BetterOptions(Set<Option> options){
        this.options = options;
    }

    public static BetterOptions fromDefaults(){
        return new BetterOptions(Collections.emptySet());
    }

    public static BetterOptions from(Option... options){
        return new BetterOptions(new HashSet<>(Arrays.asList(options)));
    }

    boolean enabled(Option option){
        return options.contains(option);
    }

    boolean arraySupport(){
        return enabled(Option.ARRAY_SUPPORT);
    }

}
