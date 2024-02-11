package io.github.chaogeoop.base.business.mongodb;

import javax.annotation.Nullable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SplitCollectionHelper {
    private static final String SPLIT_SYMBOL = "_split_";
    private static final String splitRegex = String.format("%s.*$", SPLIT_SYMBOL);
    private static final Pattern splitPattern = Pattern.compile(splitRegex);

    protected static String calBaseCollectionName(String collectionName) {
        Matcher matcher = splitPattern.matcher(collectionName);
        if (!matcher.find()) {
            return collectionName;
        }

        return collectionName.replaceAll(splitRegex, "");
    }

    @Nullable
    protected static String calSplitIndex(String collectionName) {
        Matcher matcher = splitPattern.matcher(collectionName);
        if (!matcher.find()) {
            return null;
        }

        return matcher.group().replace(SPLIT_SYMBOL, "");
    }

    public static boolean isClazzRelativeCollection(String collectionName, Class<? extends BaseModel> clazz) {
        String baseCollectionName = BaseModel.getBaseCollectionNameByClazz(clazz);
        if (collectionName.equals(baseCollectionName)) {
            return true;
        }

        if (collectionName.startsWith(String.format("%s%s", baseCollectionName, SPLIT_SYMBOL))) {
            return true;
        }

        return false;
    }

    public static String combineNameWithSplitIndex(String name, String splitIndex) {
        return String.format("%s%s%s", name, SPLIT_SYMBOL, splitIndex);
    }
}
