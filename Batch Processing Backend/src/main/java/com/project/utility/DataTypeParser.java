package com.project.utility;

import org.springframework.stereotype.Component;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;

@Component
public class DataTypeParser {

    private final List<Function<String, Object>> FUNCTIONS =
            Arrays.asList(this::tryInteger, this::tryDecimal, this::tryDate);
    private Map<String,String> DATE_FORMAT_REGEXPS = new HashMap<>();

    public DataTypeParser(){
        DATE_FORMAT_REGEXPS.put("yyyy-MM-dd","\\d{4}-(0?[1-9]|1[0-2])-(0?[1-9]|[12][0-9]|3[01])");
        DATE_FORMAT_REGEXPS.put("yyyy/MM/dd","\\d{4}/(0?[1-9]|1[0-2])/(0?[1-9]|[12][0-9]|3[01])");
        DATE_FORMAT_REGEXPS.put("MM-dd-yyyy","(0?[1-9]|1[0-2])-(0?[1-9]|[12][0-9]|3[01])-\\d{4}");
        DATE_FORMAT_REGEXPS.put("MM/dd/yyyy","(0?[1-9]|1[0-2])/(0?[1-9]|[12][0-9]|3[01])/\\d{4}");
        DATE_FORMAT_REGEXPS.put("dd-MM-yyyy","(0?[1-9]|[12][0-9]|3[01])-(0?[1-9]|1[0-2])-\\d{4}");
        DATE_FORMAT_REGEXPS.put("dd/MM/yyyy","(0?[1-9]|[12][0-9]|3[01])/(0?[1-9]|1[0-2])/\\d{4}");
    }


    private Object tryDate(String dateString) {
        for (Map.Entry<String, String> entry : DATE_FORMAT_REGEXPS.entrySet()) {
            if (dateString.toLowerCase().matches(entry.getValue())) {
                SimpleDateFormat dateFormat = new SimpleDateFormat(entry.getKey());
                try {
                    return dateFormat.parse(dateString);
                } catch (ParseException e) {
                    return null;
                }
            }
        }
        return null;
    }

    private Object tryDecimal(String value) {
        String decimalRegex = "[+-]?\\d+(\\.\\d+)?([Ee][+-]?\\d+)?";
        if (value.toLowerCase().matches(decimalRegex)) {
            try{
                return Float.parseFloat(value);
            }catch(NumberFormatException e){
                return Double.parseDouble(value);
            }
        }
        return null;
    }

    private Object tryInteger(String value) {
        String integerRegex = "[+-]?\\d+";
        if (value.toLowerCase().matches(integerRegex)) {
            try{
                return Integer.parseInt(value);
            }catch(NumberFormatException e){
                return Long.parseLong(value);
            }
        }
        return null;
    }

    public Object stringToDataType(String valueAsString) {
        if (valueAsString.isBlank()) return null;
        return FUNCTIONS.stream()
                .map(f -> f.apply(valueAsString))
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(valueAsString);
    }


}
