package com.tqy;

public class MyCommandBuilderUtils {


   public static String quoteForBatchScript(String arg) {

        boolean needsQuotes = false;
        for (int i = 0; i < arg.length(); i++) {
            int c = arg.codePointAt(i);
            if (Character.isWhitespace(c) || c == '"' || c == '=' || c == ',' || c == ';') {
                needsQuotes = true;
                break;
            }
        }
        if (!needsQuotes) {
            return arg;
        }
        StringBuilder quoted = new StringBuilder();
        quoted.append("\"");
        for (int i = 0; i < arg.length(); i++) {
            int cp = arg.codePointAt(i);
            switch (cp) {
                case '"':
                    quoted.append('"');
                    break;

                default:
                    break;
            }
            quoted.appendCodePoint(cp);
        }
        if (arg.codePointAt(arg.length() - 1) == '\\') {
            quoted.append("\\");
        }
        quoted.append("\"");
        return quoted.toString();
    }
}
